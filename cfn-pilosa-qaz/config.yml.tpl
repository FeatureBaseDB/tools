region: us-east-1
project: cfn-pilosa-qaz-cody

global:

stacks:
  pilosa:
    source: templates/pilosa.yml
    profile: sandbox
    cf:
      agents: 1
      nodes: 3
    parameters:
      - ClusterName: codytest0
      - KeyPair: cody@soyland.org
      - Subnet: subnet-839804cb
      - VPC: vpc-2b885052
