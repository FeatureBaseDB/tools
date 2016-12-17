.PHONY: create-stack update-stack delete-stack describe-stack upload-ssh-key
CLUSTER_SIZE=3
NUM_AGENTS=1
STACK=cluster0
INSTANCE_TYPE=m3.medium
AMI=ami-e3c3b8f4
VPC=vpc-a04897c6
SUBNET=subnet-5ce57307
PARAMS=ParameterKey=ClusterName,ParameterValue=$(STACK) ParameterKey=KeyPair,ParameterValue=$(KEY_PAIR) ParameterKey=InstanceType,ParameterValue=$(INSTANCE_TYPE) ParameterKey=AMI,ParameterValue=$(AMI) ParameterKey=VPC,ParameterValue=$(VPC) ParameterKey=Subnet,ParameterValue=$(SUBNET)
PY_FILE=cfn-pilosa.py
JSON_FILE=build/cfn-pilosa-$(CLUSTER_SIZE)-$(NUM_AGENTS).json

help:                                            ## Show this help
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

env:                                             ## Create virtualenv
	virtualenv env

env/deps: requirements.txt env                   ## Install dependencies
	env/bin/pip install -r requirements.txt && touch env/deps

$(JSON_FILE): env/deps $(PY_FILE) ## Create Cloudformation template
	env/bin/python $(PY_FILE) $(CLUSTER_SIZE) $(NUM_AGENTS) > $(JSON_FILE)

create-stack: $(JSON_FILE) env/deps              ## Create Pilosa cluster
	test -n "$(KEY_PAIR)" # Required variable KEY_PAIR
	env/bin/aws cloudformation create-stack --stack-name $(STACK) --template-body file://$(JSON_FILE) --capabilities CAPABILITY_IAM --parameters $(PARAMS)

update-stack: $(JSON_FILE) env/deps              ## Update Pilosa cluster
	test -n "$(KEY_PAIR)" # Required variable KEY_PAIR
	env/bin/aws cloudformation update-stack --stack-name $(STACK) --template-body file://$(JSON_FILE) --capabilities CAPABILITY_IAM --parameters $(PARAMS)

delete-stack: env/deps                           ## Delete Pilosa cluster
	env/bin/aws cloudformation delete-stack --stack-name $(STACK)

describe-stack: env/deps                         ## Describe Cloudformation stack
	env/bin/aws cloudformation describe-stacks --stack-name $(STACK)

upload-ssh-key: env/deps                         ## Upload SSH key-pair to AWS
	bash upload-ssh-key.sh
