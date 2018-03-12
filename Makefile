purge-varnish-cache: main.go
	 go build

prod-release:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w"
	upx purge-varnish-cache

deps:
	go get
