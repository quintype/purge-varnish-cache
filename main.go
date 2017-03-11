package main

import (
	"flag"
	"fmt"
	"os"

	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func getOrCreateQueue(svc *sqs.SQS, name string) (*string) {
	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			_, err := svc.CreateQueue(&sqs.CreateQueueInput{
				QueueName: aws.String(name),
				Attributes: map[string]*string{
					"DelaySeconds":           aws.String("0"),
					"MessageRetentionPeriod": aws.String("3600"),
				},
			})

			if err != nil {
				exitErrorf("Unable to create queue %q.", name)
			}

			return getOrCreateQueue(svc, name)
		}
		exitErrorf("Unable to queue %q, %v.", name, err)
	}

	return resultURL.QueueUrl
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

	fmt.Printf("Received %d messages.\n", len(result.Messages))
	return result.Messages;
}

func processMessages(server string, messages [] *sqs.Message) {
	client := http.Client{}

	for i := range messages {
		key := messages[i].Body
		request, err := http.NewRequest("BAN", server, nil)
		if err != nil {
			fmt.Println("Unable to create purge request", *key, err);
			return;
		}

		request.Header.Add("Surrogate-Key", *key)

		_, err = client.Do(request)

		if err != nil {
			fmt.Println("Unable to purge", *key, err);
			return;
		}

		fmt.Printf("Purged %s\n", *key)
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
	var name, server, region string
	var timeout int64
	flag.StringVar(&name, "n", "", "Queue name")
	flag.StringVar(&server, "s", "http://localhost:8080", "Server Connection String")
	flag.StringVar(&region, "r", "us-east-1", "AWS region")
	flag.Int64Var(&timeout, "t", 20, "(Optional) Timeout in seconds for long polling")
	flag.Parse()

	if len(name) == 0 {
		flag.PrintDefaults()
		exitErrorf("Queue name required")
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String(region)},
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create a SQS service client.
	svc := sqs.New(sess)

	queueUrl := getOrCreateQueue(svc, name)

	for {
		messages := recieveMessages(svc, queueUrl, timeout)
		if(len(messages) > 0) {
			deleteMessages(svc, queueUrl, messages)
			processMessages(server, messages);
		}
	}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
