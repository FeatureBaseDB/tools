from __future__ import print_function
from troposphere import route53, iam, ec2, Ref, Template, GetAtt, Base64, Parameter, Tags, Sub
from troposphere.iam import Role, InstanceProfile
from troposphere_sugar.decorators import cfparam, cfresource
from troposphere_sugar import Skel
from troposphere import route53, Ref, Parameter, Join
from awacs.aws import Allow, Statement, Principal, Policy
from awacs.sts import AssumeRole
from functools import partial
from textwrap import dedent
import sys

class PilosaTemplate(Skel):
    def __init__(self, cluster_size, num_agents, goversion, username, domain):
        super(PilosaTemplate, self).__init__()
        self.cluster_size = cluster_size
        self.num_agents = num_agents
        self.goversion = goversion
        self.username = username
        self.domain = domain
        self.common_user_data = dedent("""
                #!/bin/bash

                # install prereqs
                apt-get update
                apt-get -y install git
                apt-get -y install make

                # install go
                mkdir -p /usr/local/go
                wget https://storage.googleapis.com/golang/{goversion}.tar.gz
                tar -C /usr/local -xzf {goversion}.tar.gz
                chown -R {username}:{username} /usr/local/go
                mkdir -p /home/{username}/go/src/github.com/pilosa
                mkdir -p /home/{username}/go/bin
                GOPATH=/home/{username}/go
                export GOPATH
                PATH=$PATH:/usr/local/go/bin:$GOPATH/bin
                export PATH

                # set up GOPATH in .bashrc
                cat >> /home/{username}/.bashrc <<- EOF
                GOPATH=/home/{username}/go
                export GOPATH
                PATH=$PATH:/usr/local/go/bin:$GOPATH/bin
                export PATH
                EOF
"""[1:]).format(goversion=goversion, username=username)
        sys.stderr.write(self.common_user_data)



    @cfparam
    def vpc(self):
        return Parameter(
            'VPC',
            Description='VPC to use for pilosa instance',
            Type='String',
        )

    @cfparam
    def subnet(self):
        return Parameter(
            'Subnet',
            Description='Subnet to use for pilosa instance',
            Type='String',
        )

    @cfparam
    def ami(self):
        return Parameter(
            'AMI',
            Description='AMI to use for pilosa instance',
            Type='String',
            Default='ami-e3c3b8f4',
        )

    @cfparam
    def key_pair(self):
        return Parameter(
            'KeyPair',
            Description='Key pair to use for sudoer user',
            Type='String',
        )

    @cfparam
    def instance_type(self):
        return Parameter(
            'InstanceType',
            Description='Instance type of pilosa',
            Type='String',
            Default='m3.medium',
        )

    @cfparam
    def agent_instance_type(self):
        return Parameter(
            'AgentInstanceType',
            Description='Instance type of agent nodes',
            Type='String',
            Default='c4.large',
        )

    @cfparam
    def cluster_name(self):
        return Parameter(
            'ClusterName',
            Description='Unique name for this pilosa cluster. Used in DNS (node0.{{name}}.{domain}'.format(domain=self.domain),
            Type='String',
            Default='cluster0',
        )

    @cfresource
    def role(self):
        return Role(
            "PilosaRole",
            AssumeRolePolicyDocument=Policy(
                Statement=[
                    Statement(
                        Effect=Allow,
                        Action=[AssumeRole],
                        Principal=Principal("Service", ["ec2.amazonaws.com"])
                    )
                ]
            ),
            Policies=[iam.Policy(
                PolicyName='PilosaS3Policy',
                PolicyDocument={
                  "Version": "2012-10-17",
                  "Statement": [
                    {
                      "Effect": "Allow",
                      "Action": ["s3:*"],
                      "Resource": ["arn:aws:s3:::dist.pilosa.com", "arn:aws:s3:::dist.pilosa.com/*"]
                    }
                  ]
                }
            )],
        )

    @cfresource
    def instance_profile(self):
        return InstanceProfile(
            "PilosaInstanceProfile",
            Roles=[Ref(self.role)]
        )

    @cfresource
    def hosted_zone(self):
        return route53.HostedZone(
            'PilosaZone',
            Name=Join('', [Ref(self.cluster_name), '.{domain}'.format(domain=self.domain)]),
            VPCs=[route53.HostedZoneVPCs(VPCId=Ref(self.vpc), VPCRegion=Ref('AWS::Region'))])

    @cfresource
    def instance_security_group(self):
        return ec2.SecurityGroup(
            'PilosaInstanceSecurityGroup',
            GroupDescription='Enable SSH access via port 22',
            SecurityGroupIngress=[
                ec2.SecurityGroupRule(
                    IpProtocol='tcp',
                    FromPort='22',
                    ToPort='22',
                    CidrIp='0.0.0.0/0',
                ),
            ],
            VpcId=Ref(self.vpc),
        )

    @cfresource
    def instance_security_group_ingress(self):
        return ec2.SecurityGroupIngress(
            "PilosaIngress",
            IpProtocol='tcp',
            FromPort='10101',
            ToPort='10101',
            GroupId=Ref(self.instance_security_group),
            SourceSecurityGroupId=Ref(self.instance_security_group),
        )

    def instance(self, index):
        config_file = dedent('''
            data-dir = "/tmp/pil0"
            host = "node{node}.{stack_name}.{domain}:10101"

            [cluster]
            replicas = {count}

            ''')[1:].format(node=index, count=self.cluster_size, stack_name='${AWS::StackName}', domain=self.domain)

        for node in range(self.cluster_size):
            config_file += dedent('''
                [[cluster.node]]
                host = "node{node}.{stack_name}.{domain}:10101"

                '''[1:]).format(node=node, stack_name='${AWS::StackName}', domain=self.domain)

        user_data = dedent('''
                {common}

                # update open file limits
                cat >> /etc/security/limits.conf <<- EOF
                * soft nofile 262144
                * hard nofile 262144
                * hard memlock unlimited
                * soft memlock unlimited
                EOF

                # install pilosa
                go get -u github.com/pilosa/pilosa
                cd $GOPATH/src/github.com/pilosa/pilosa
                make install

                # set up pilosa config file
                cat > /etc/pilosa.cfg <<- EOF
                {config_file}
                EOF

                # clean up root's mess
                chown -R {username}:{username} /home/{username}
                '''[1:]).format(config_file=config_file, common=self.common_user_data, username=self.username)

        return ec2.Instance(
            'PilosaInstance{}'.format(index),
            ImageId = Ref(self.ami), #ubuntu
            InstanceType = Ref(self.instance_type),
            KeyName = Ref(self.key_pair),
            IamInstanceProfile=Ref(self.instance_profile),
            NetworkInterfaces=[
                ec2.NetworkInterfaceProperty(
                    GroupSet=[Ref(self.instance_security_group.title)],
                    AssociatePublicIpAddress='true',
                    DeviceIndex='0',
                    DeleteOnTermination='true',
                    SubnetId=Ref(self.subnet))],

            UserData = Base64(Sub(user_data)),
        )

    def agent_instance(self, index):
        user_data = dedent('''
                {common}

                apt-get -y install gcc
                apt-get -y install libpcap-dev

                # install pdk
                go get -u github.com/pilosa/pdk
                cd $GOPATH/src/github.com/pilosa/pdk
                make install

                # clean up root's mess
                chown -R {username}:{username} /home/{username}
                '''[1:]).format(common=self.common_user_data, username=self.username)
        return ec2.Instance(
            'PilosaAgentInstance{}'.format(index),
            ImageId=Ref(self.ami), # ubuntu
            InstanceType=Ref(self.agent_instance_type),
            KeyName=Ref(self.key_pair),
            IamInstanceProfile=Ref(self.instance_profile),
            NetworkInterfaces=[
                ec2.NetworkInterfaceProperty(
                    GroupSet=[Ref(self.instance_security_group.title)],
                    AssociatePublicIpAddress='true',
                    DeviceIndex='0',
                    DeleteOnTermination='true',
                    SubnetId=Ref(self.subnet))],

            UserData=Base64(Sub(user_data)),
        )

    def public_record_set(self, index):
        return route53.RecordSetType(
            'PilosaPublicRecordSet{}'.format(index),
            HostedZoneName='{domain}.'.format(domain=self.domain),
            Name=Join('', ['node{}.'.format(index), Ref(self.cluster_name), '.{domain}.'.format(domain=self.domain)]),
            Type="A",
            TTL="300",
            ResourceRecords=[GetAtt("PilosaInstance{}".format(index), "PublicIp")],
        )

    def agent_public_record_set(self, index):
        return route53.RecordSetType(
            'AgentPublicRecordSet{}'.format(index),
            HostedZoneName='{domain}.'.format(domain=self.domain),
            Name=Join('', ['agent{}.'.format(index), Ref(self.cluster_name), '.{domain}.'.format(domain=self.domain)]),
            Type="A",
            TTL="300",
            ResourceRecords=[GetAtt("PilosaAgentInstance{}".format(index), "PublicIp")],
        )

    def private_record_set(self, index):
        return route53.RecordSetType(
            'PilosaPrivateRecordSet{}'.format(index),
            HostedZoneId=Ref(self.hosted_zone),
            Name=Join('', ['node{}.'.format(index), Ref(self.cluster_name), '.{domain}.'.format(domain=self.domain)]),
            Type="A",
            TTL="300",
            ResourceRecords=[GetAtt("PilosaInstance{}".format(index), "PrivateIp")],
        )

    def agent_private_record_set(self, index):
        return route53.RecordSetType(
            'AgentPrivateRecordSet{}'.format(index),
            HostedZoneId=Ref(self.hosted_zone),
            Name=Join('', ['agent{}.'.format(index), Ref(self.cluster_name), '.{domain}.'.format(domain=self.domain)]),
            Type="A",
            TTL="300",
            ResourceRecords=[GetAtt("PilosaAgentInstance{}".format(index), "PrivateIp")],
        )

    def process(self):
        super(PilosaTemplate, self).process()
        for i in range(self.cluster_size):
            self.template.add_resource(self.instance(i))
            self.template.add_resource(self.public_record_set(i))
            self.template.add_resource(self.private_record_set(i))
        for i in range(self.num_agents):
            self.template.add_resource(self.agent_instance(i))
            self.template.add_resource(self.agent_public_record_set(i))
            self.template.add_resource(self.agent_private_record_set(i))

def main():
    cluster_size = 3
    if len(sys.argv) > 1:
        cluster_size = int(sys.argv[1])
    num_agents = 1
    if len(sys.argv) > 2:
        num_agents = int(sys.argv[2])
    goversion = "go1.8.3.linux-amd64"
    if len(sys.argv) > 3:
        goversion = sys.argv[3]
    username = "ubuntu"
    if len(sys.argv) > 4:
        username = sys.argv[4]
    domain = "sandbox.pilosa.com"
    if len(sys.argv) > 5:
        domain = sys.argv[5]
    print(PilosaTemplate(cluster_size=cluster_size,
                         num_agents=num_agents,
                         goversion=goversion,
                         username=username,
                         domain=domain).output)

if __name__ == '__main__':
    main()
