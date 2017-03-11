purge-varnish-cache: main.go
	GOPATH=`pwd`/gopath go build

deps:
	GOPATH=`pwd`/gopath go get
