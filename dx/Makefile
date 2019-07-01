
build:
	GO111MODULE=on go build -o dx main.go

ingest:
	dx ingest --cport=10102 --pport=10103 --threadcount=1

ingest4:
	dx ingest --cport=10102 --pport=10103 --threadcount=4

query:
	dx query --cport=10102 --pport=10103 --queries=100,1000,10000 --threadcount=1

query4:
	dx query --cport=10102 --pport=10103 --queries=100,1000,10000 --threadcount=4

server:
	${MAKE} -j2 candidate primary

candidate:
	$(GOPATH)/bin/pilosa server --bind="localhost:10102" --gossip.port=14002 --data-dir="~/.pilosa/dx/candidate"

primary:
	pilosa server --bind="localhost:10103" --gossip.port=14003 --data-dir="~/.pilosa/dx/primary"
