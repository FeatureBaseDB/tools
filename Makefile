VERSION := $(shell git rev-parse --short HEAD 2> /dev/null || echo unknown)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
BUILD_TIME=`date -u +%FT%T%z`
LDFLAGS="-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"
CLONE_URL=github.com/pilosa/tools


pitool: vendor
	go install -ldflags $(LDFLAGS) $(CLONE_URL)/cmd/pitool
