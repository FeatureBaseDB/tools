
build:
	go build -o /Users/shaquillewyanque/git/pilosa/tools/dx/dx /Users/shaquillewyanque/git/pilosa/tools/dx/cmd

test:
	dx && dx ingest && dx query && dx all

candidate:
	$(GOPATH)/bin/pilosa server --bind="localhost:10102" --gossip.port=14002 --data-dir="~/.pilosa/dx/candidate"

primary:
	pilosa server --bind="localhost:10103" --gossip.port=14003 --data-dir="~/.pilosa/dx/primary"
