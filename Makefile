VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
BUILD_TIME=`date -u +%FT%T%z`
LDFLAGS="-X github.com/pilosa/pilosa.Version=$(VERSION) -X github.com/pilosa/pilosa.BuildTime=$(BUILD_TIME)"
CLONE_URL=github.com/pilosa/tools


pitool: vendor
	go install -ldflags $(LDFLAGS) $(CLONE_URL)/cmd/pitool
