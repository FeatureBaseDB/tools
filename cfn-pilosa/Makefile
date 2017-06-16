.PHONY: create-stack create-stack-complete update-stack delete-stack describe-stack upload-ssh-key
CLUSTER_SIZE=3
NUM_AGENTS=1
STACK=cluster0
INSTANCE_TYPE=m3.medium
AGENT_INSTANCE_TYPE=m3.medium
AMI=ami-e3c3b8f4
VPC=vpc-2b885052
SUBNET=subnet-839804cb
DOMAIN=sandbox.pilosa.com
USERNAME=ubuntu
GOVERSION=go1.8.3.linux-amd64
REPLICAS=1
VOLUME_SIZE=10
VOLUME_TYPE=gp2
PARAMS=ParameterKey=ClusterName,ParameterValue=$(STACK) ParameterKey=KeyPair,ParameterValue=$(KEY_PAIR) ParameterKey=InstanceType,ParameterValue=$(INSTANCE_TYPE) ParameterKey=AgentInstanceType,ParameterValue=$(AGENT_INSTANCE_TYPE) ParameterKey=AMI,ParameterValue=$(AMI) ParameterKey=VPC,ParameterValue=$(VPC) ParameterKey=Subnet,ParameterValue=$(SUBNET) ParameterKey=VolumeSize,ParameterValue=$(VOLUME_SIZE) ParameterKey=VolumeType,ParameterValue=$(VOLUME_TYPE)
PY_FILE=cfn-pilosa.py
JSON_FILE=build/cfn-pilosa-$(CLUSTER_SIZE)-$(NUM_AGENTS).json
BENCHMARK_CONFIG=$(GOPATH)/src/github.com/pilosa/tools/cmd/pitool/spawn.json
CLUSTER_HOSTS=`python -c "print \",\".join([\"node{}.$(STACK).$(DOMAIN):10101\".format(i) for i in range($(CLUSTER_SIZE))])"`
AGENT_HOSTS=`python -c "print \",\".join([\"agent{}.$(STACK).$(DOMAIN)\".format(i) for i in range($(NUM_AGENTS))])"`

help:                                            ## Show this help
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

env:                                             ## Create virtualenv
	virtualenv env

env/deps: requirements.txt env                   ## Install dependencies
	env/bin/pip install -r requirements.txt && touch env/deps

$(JSON_FILE): env/deps $(PY_FILE) ## Create Cloudformation template
	env/bin/python $(PY_FILE) $(CLUSTER_SIZE) $(NUM_AGENTS) $(GOVERSION) $(USERNAME) $(DOMAIN) $(REPLICAS) > $(JSON_FILE)

create-stack: $(JSON_FILE) env/deps              ## Create Pilosa cluster
	test -n "$(KEY_PAIR)" # Required variable KEY_PAIR
	env/bin/aws cloudformation create-stack --stack-name $(STACK) --template-body file://$(JSON_FILE) --capabilities CAPABILITY_IAM --parameters $(PARAMS)

create-stack-complete: create-stack		 ## Create Pilosa cluster and wait for completion
	env/bin/aws cloudformation wait stack-create-complete --stack-name $(STACK)

run-benchmarks: create-stack-complete	         ## Create Pilosa cluster, run benchmarks, and shut down
	go install github.com/pilosa/tools/cmd/pitool
	pitool bspawn -creator.hosts=$(CLUSTER_HOSTS) -agent-hosts=$(AGENT_HOSTS) -ssh-user=ubuntu -human -copy-binary -creator.log-file-prefix=clusterlogs $(BENCHMARK_CONFIG)
	make delete-stack STACK=$(STACK)

update-stack: $(JSON_FILE) env/deps              ## Update Pilosa cluster
	test -n "$(KEY_PAIR)" # Required variable KEY_PAIR
	env/bin/aws cloudformation update-stack --stack-name $(STACK) --template-body file://$(JSON_FILE) --capabilities CAPABILITY_IAM --parameters $(PARAMS)

delete-stack: env/deps                           ## Delete Pilosa cluster
	env/bin/aws cloudformation delete-stack --stack-name $(STACK)

describe-stack: env/deps                         ## Describe Cloudformation stack
	env/bin/aws cloudformation describe-stacks --stack-name $(STACK)

upload-ssh-key: env/deps                         ## Upload SSH key-pair to AWS
	bash upload-ssh-key.sh
