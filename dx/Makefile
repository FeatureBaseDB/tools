
build:
	go build -o dx ./main.go

ingest:
	dx ingest --cPort=10102 --pPort=10103 --threadCount=1

query:
	dx query --cPort=10102 --pPort=10103 --queries=100,1000,10000 --threadCount=1

server:
	${MAKE} -j2 candidate primary

candidate:
	$(GOPATH)/bin/pilosa server --bind="localhost:10102" --gossip.port=14002 --data-dir="~/.pilosa/dx/candidate"

primary:
	pilosa server --bind="localhost:10103" --gossip.port=14003 --data-dir="~/.pilosa/dx/primary"
