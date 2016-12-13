from troposphere import route53, iam, ec2, Ref, Template, GetAtt, Base64, Parameter, Tags
from troposphere.iam import Role, InstanceProfile
from troposphere_sugar.decorators import cfparam, cfresource
from troposphere_sugar import Skel
from troposphere import route53, Ref, Parameter, Join
from awacs.aws import Allow, Statement, Principal, Policy
from awacs.sts import AssumeRole
from functools import partial
from textwrap import dedent

class PilosaTemplate(Skel):
    def __init__(self, machine_count):
        super(PilosaTemplate, self).__init__()
        self.machine_count = machine_count

    @cfparam
    def ami(self):
        return Parameter(
            'AMI',
            Description='AMI to use for pilosa instance',
            Type='String',
            Default='ami-e3c3b8f4',
        )

    @cfparam
    def keypair(self):
        return Parameter(
            'Keypair',
            Description='Keypair to use for sudoer user',
            Type='String',
            Default='cody@soyland.org',
        )

    @cfparam
    def instance_type(self):
        return Parameter(
            'InstanceType',
            Description='Instance type of pilosa',
            Type='String',
            Default='m4.large',
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
    def vpc(self):
        return ec2.VPC(
            'PilosaVPC',
            InstanceTenancy='dedicated',
            CidrBlock='10.0.0.0/16',
            EnableDnsHostnames='true',
            EnableDnsSupport='true',
        )

    @cfresource
    def hosted_zone(self):
        return route53.HostedZone(
            'PilosaZone',
            Name='cluster0.sandbox.pilosa.com',
            VPCs=[route53.HostedZoneVPCs(VPCId=Ref(self.vpc), VPCRegion=Ref('AWS::Region'))])

    @cfresource
    def subnet(self):
        return ec2.Subnet(
            'Subnet',
            CidrBlock='10.0.0.0/24',
            VpcId=Ref(self.vpc),
            Tags=Tags(Application=Ref('AWS::StackId')))

    @cfresource
    def gateway(self):
        return ec2.InternetGateway('InternetGateway')

    @cfresource
    def gateway_attachment(self):
        return ec2.VPCGatewayAttachment(
            'AttachGateway',
            VpcId=Ref(self.vpc),
            InternetGatewayId=Ref(self.gateway))

    @cfresource
    def route_table(self):
        return ec2.RouteTable(
            'RouteTable',
            VpcId=Ref(self.vpc))

    @cfresource
    def internet_route(self):
        return ec2.Route(
            'Route',
            DependsOn='AttachGateway',
            GatewayId=Ref(self.gateway),
            DestinationCidrBlock='0.0.0.0/0',
            RouteTableId=Ref(self.route_table))

    @cfresource
    def subnet_route_table_association(self):
        return ec2.SubnetRouteTableAssociation(
            'SubnetRouteTableAssociation',
            SubnetId=Ref(self.subnet),
            RouteTableId=Ref(self.route_table))

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
                    #SourceSecurityGroupId=Ref(self.security_group().title)
                ),
            ],
            VpcId=Ref(self.vpc),
        )

    @cfresource
    def instance_security_group_ingress(self):
        return ec2.SecurityGroupIngress(
            "PilosaIngress",
            IpProtocol='tcp',
            FromPort='15000',
            ToPort='15000',
            GroupId=Ref(self.instance_security_group),
            SourceSecurityGroupId=Ref(self.instance_security_group),
        )

    def instance(self, index):
        return ec2.Instance(
            'PilosaInstance{}'.format(index),
            ImageId = Ref(self.ami), #ubuntu
            InstanceType = Ref(self.instance_type),
            KeyName = Ref(self.keypair),
            IamInstanceProfile=Ref(self.instance_profile),
            NetworkInterfaces=[
                ec2.NetworkInterfaceProperty(
                    GroupSet=[Ref(self.instance_security_group.title)],
                    AssociatePublicIpAddress='true',
                    DeviceIndex='0',
                    DeleteOnTermination='true',
                    SubnetId=Ref(self.subnet))],

            UserData = Base64(dedent('''
                #!/bin/bash
                echo "Now we download Pilosa!!!"
                # TODO: download pilosa binary, run it
            '''[1:])), # strip leading newline and dedent
        )

    def public_record_set(self, index):
        return route53.RecordSetType(
            'PilosaPublicRecordSet{}'.format(index),
            #HostedZoneId=Ref(self.hosted_zone),
            HostedZoneName='sandbox.pilosa.com.',
            Name='pilosa{}.cluster0.sandbox.pilosa.com.'.format(index),
            Type="A",
            TTL="300",
            ResourceRecords=[GetAtt("PilosaInstance{}".format(index), "PublicIp")],
        )

    def private_record_set(self, index):
        return route53.RecordSetType(
            'PilosaPrivateRecordSet{}'.format(index),
            HostedZoneId=Ref(self.hosted_zone),
            Name='pilosa{}.cluster0.sandbox.pilosa.com.'.format(index),
            Type="A",
            TTL="300",
            ResourceRecords=[GetAtt("PilosaInstance{}".format(index), "PrivateIp")],
        )

    def process(self):
        super(PilosaTemplate, self).process()
        for i in range(self.machine_count):
            self.template.add_resource(self.instance(i))
            self.template.add_resource(self.public_record_set(i))
            self.template.add_resource(self.private_record_set(i))

def main():
    print PilosaTemplate(machine_count=3).output

if __name__ == '__main__':
    main()
