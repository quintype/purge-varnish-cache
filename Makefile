purge-varnish-cache: main.go
	 go build

prod-release:
	GOOS=linux GOARCH=amd64 GOPATH=`pwd`/gopath go build -ldflags="-s -w"
	upx purge-varnish-cache

deps:
	GOPATH=`pwd`/gopath go get
