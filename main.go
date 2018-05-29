package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"net/http"
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sns"
)

const AppVersion = "1.2.0"

func getOrCreateQueue(svc *sqs.SQS, region, name, accountId, topicARN string) (*string) {
	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})


	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			sqsArn := arn.ARN{
				Partition: "aws",
				Service: "sqs",
				Region: region,
				AccountID: accountId,
				Resource: name,
			}

			policy := fmt.Sprintf("{\"Version\":\"2012-10-17\",\"Id\":\"SQSDefaultPolicy\",\"Statement\":[{\"Sid\":\"Sid123123123123\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"},\"Action\":\"SQS:SendMessage\",\"Resource\":\"%s\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"%s\"}}}]}",sqsArn, topicARN)

			_, err := svc.CreateQueue(&sqs.CreateQueueInput{
				QueueName: aws.String(name),
				Attributes: map[string]*string{
					"DelaySeconds":           aws.String("0"),
					"MessageRetentionPeriod": aws.String("3600"),
					"Policy":                 aws.String(policy),
				},
			})

			if err != nil {
				exitErrorf("Unable to create queue %q.", name)
			}

			return getOrCreateQueue(svc, region, name, accountId, topicARN)
		}
		exitErrorf("Unable to queue %q, %v.", name, err)
	}

	return resultURL.QueueUrl
}


func getQueueArn(svc *sqs.SQS, queueUrl *string) (*string) {

	queueArn := "QueueArn";

	result, err := svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl: queueUrl,
		AttributeNames: []*string{&queueArn},
	})

	if err != nil {
		exitErrorf("Couldn't get sqs Attributes.", err)
	}

	arn := result.Attributes[queueArn];

	if arn == nil {
		exitErrorf("Got nil sqs ARN", err)
	}

	return arn;
}



func recieveMessages(svc *sqs.SQS, queueUrl *string, timeout int64) ([]*sqs.Message) {
	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl: queueUrl,
		AttributeNames: aws.StringSlice([]string{
			"SentTimestamp",
		}),
		MaxNumberOfMessages: aws.Int64(10),
		MessageAttributeNames: aws.StringSlice([]string{
			"All",
		}),
		WaitTimeSeconds: aws.Int64(timeout),
	})
	if err != nil {
		exitErrorf("Unable to receive message from queue %q, %v.", queueUrl, err)
	}

	if len(result.Messages) > 0 {
		fmt.Printf("Received %d messages.\n", len(result.Messages))
	}

	return result.Messages;
}

func processMessages(server string, messages [] *sqs.Message) {
	client := http.Client{}

	type Message struct {
		Message, Subject string
	}

	for i := range messages {
		body := *messages[i].Body
		dec := json.NewDecoder(strings.NewReader(body))

		var m Message
		if dec.Decode(&m) != nil {
			fmt.Println("Unable to parse json", body);
			return;
		}
		key := m.Message

		request, err := http.NewRequest("BAN", server, nil)
		if err != nil {
			fmt.Println("Unable to create purge request", key, err);
			return;
		}

		request.Header.Add("Surrogate-Key", key)
		request.Header.Set("Connection","close")

		resp, err := client.Do(request);

		if err != nil {
			fmt.Println("Unable to purge", key, err);
			return;
		}

		fmt.Printf("Purged %s\n", key)
		defer resp.Body.Close()
	}
}

func deleteMessages(svc *sqs.SQS, queueUrl *string, messages [] *sqs.Message) {
	entries := make([]*sqs.DeleteMessageBatchRequestEntry, len(messages));

	for i := 0; i < len(messages); i++ {
		entries[i] = &sqs.DeleteMessageBatchRequestEntry{
			Id: messages[i].MessageId,
			ReceiptHandle: messages[i].ReceiptHandle,
		}
	}

	_, err := svc.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		QueueUrl: queueUrl,
		Entries: entries,
	})

	if err != nil {
		exitErrorf("Unable to delete messages", err)
	}
}

func main() {
	var name, topicARN, accountId, server, region string
	var timeout int64
	var version bool
	flag.StringVar(&name, "n", "", "Queue name")
	flag.StringVar(&topicARN, "sns", "", "SNS ARN")
	flag.StringVar(&accountId, "a", "Sid2133213213", "Your AccountId")
	flag.StringVar(&server, "s", "http://localhost:6081", "Server Connection String")
	flag.StringVar(&region, "r", "us-east-1", "AWS region")
	flag.Int64Var(&timeout, "t", 20, "(Optional) Timeout in seconds for long polling")
	flag.BoolVar(&version, "v", false, "Prints the current version of the app")
	flag.Parse()

	if version {
		fmt.Println(AppVersion);
		os.Exit(1);
	}

	if len(name) == 0 {
		flag.PrintDefaults()
		exitErrorf("Queue name required")
	}

	if len(topicARN) == 0 {
		flag.PrintDefaults()
		exitErrorf("SNS Topic ARN is required")
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String(region)},
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create a SQS service client.
	sqsSvc := sqs.New(sess)

	queueUrl := getOrCreateQueue(sqsSvc, region, name, accountId, topicARN)

	// Create a SNS client from just a session.

	snsClient := sns.New(sess)
	queueArn := getQueueArn(sqsSvc, queueUrl)

	_, err := snsClient.Subscribe(&sns.SubscribeInput{
		Endpoint: queueArn,
		TopicArn: &topicARN,
		Protocol: aws.String("sqs"),
	})
	
	if err != nil {
		exitErrorf("Couldn't subscribe to SNS client.", err)
	}

	for {
		messages := recieveMessages(sqsSvc, queueUrl, timeout)
		if(len(messages) > 0) {
			deleteMessages(sqsSvc, queueUrl, messages)
			processMessages(server, messages);
		}
	}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
