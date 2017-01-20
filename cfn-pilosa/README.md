cfn-pilosa
==========

This is a Cloudformation template generator for creating Pilosa clusters.

Usage
-----

### Show help

```
$ make help
```

### Create a stack

Required arguments:

- STACK: Cloudformation stack name and name of cluster in DNS
- KEY_PAIR: The name of the AWS key-pair that is set up to administer the machine

Optional arguments:

- VPC: VPC ID to use (default: vpc-a04897c6)
- SUBNET: Subnet ID to use (default: subnet-5ce57307)
- CLUSTER_SIZE: Number of instances (default: 3)
- NUM_AGENTS: Number of benchmark agents to set up (default: 1)
- INSTANCE_TYPE: Instance type in AWS (default: m3.medium)
- AMI: AWS machine image (default: ami-e3c3b8f4)

```
$ make create-stack STACK=clustername
```

### Update a stack

Same arguments as "make create-stack"

```
$ make update-stack STACK=foo
```

### Delete a stack

```
$ make delete-stack STACK=foo
```

### Describe a stack

```
$ make describe-stack STACK=foo
```

### Upload SSH key to AWS Key-pair

```
$ make upload-ssh-key
```
