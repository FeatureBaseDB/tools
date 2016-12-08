.PHONY: create-stack update-stack delete-stack describe-stack
STACK=production
PY_FILE=cfn-production.py
JSON_FILE=cfn-production.json
PARAMETERS=

env:
	virtualenv env

env/deps: requirements.txt env
	env/bin/pip install -r requirements.txt && touch env/deps

$(JSON_FILE): env/deps $(PY_FILE)
	env/bin/python $(PY_FILE) > $(JSON_FILE)

create-stack: $(JSON_FILE) env/deps
	env/bin/aws cloudformation create-stack --stack-name $(STACK) --template-body file://$(JSON_FILE) --capabilities CAPABILITY_IAM --parameters $(PARAMETERS)

update-stack: $(JSON_FILE) env/deps
	env/bin/aws cloudformation update-stack --stack-name $(STACK) --template-body file://$(JSON_FILE) --capabilities CAPABILITY_IAM

delete-stack: env/deps
	env/bin/aws cloudformation delete-stack --stack-name $(STACK)

describe-stack: env/deps
	env/bin/aws cloudformation describe-stacks --stack-name $(STACK)
