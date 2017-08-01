VERSION := $(shell git rev-parse --short HEAD 2> /dev/null || echo unknown)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
BUILD_TIME=`date -u +%FT%T%z`
CLONE_URL=github.com/pilosa/tools
LDFLAGS="-X $(CLONE_URL).Version=$(VERSION) -X $(CLONE_URL).BuildTime=$(BUILD_TIME)"


pi: vendor
	go install -ldflags $(LDFLAGS) $(CLONE_URL)/cmd/pi
