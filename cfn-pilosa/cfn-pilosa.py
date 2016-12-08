from troposphere import route53, iam, ec2, Ref, Template, GetAtt, Base64, Parameter, Tags
from troposphere.iam import Role, InstanceProfile
from awacs.aws import Allow, Statement, Principal, Policy
from awacs.sts import AssumeRole
from functools import partial
from textwrap import dedent

class memoize(object):
    def __init__(self, func):
        self.func = func
    def __get__(self, obj, objtype=None):
        if obj is None:
            return self.func
        return partial(self, obj)
    def __call__(self, *args, **kw):
        obj = args[0]
        try:
            cache = obj.__cache
        except AttributeError:
            cache = obj.__cache = {}
        key = (self.func, args[1:], frozenset(kw.items()))
        try:
            res = cache[key]
        except KeyError:
            res = cache[key] = self.func(*args, **kw)
        return res

class PilosaTemplate(object):

    @memoize
    def template(self):
        return Template()

    @memoize
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

    @memoize
    def instance_profile(self):
        return InstanceProfile(
            "PilosaInstanceProfile",
            Roles=[Ref(self.role())]
        )

    @memoize
    def vpc(self):
        return ec2.VPC(
            'PilosaVPC',
            InstanceTenancy='dedicated',
            CidrBlock='10.0.0.0/16',
            EnableDnsHostnames='true',
            EnableDnsSupport='true',
        )

    @memoize
    def hosted_zone(self):
        return route53.HostedZone(
            'PilosaZone',
            Name='servers.pilosa.com',
            VPCs=[route53.HostedZoneVPCs(VPCId=Ref(self.vpc()), VPCRegion=Ref('AWS::Region'))])

    @memoize
    def record_set(self, index):
        return route53.RecordSetType(
            'PilosaRecordSet{}'.format(index),
            HostedZoneId=Ref(self.hosted_zone()),
            Name='pilosa{}.servers.pilosa.com.'.format(index),
            Type="A",
            TTL="900",
            ResourceRecords=[GetAtt(self.instance(index), "PrivateIp")],
        )

    @memoize
    def subnet(self):
        return ec2.Subnet(
            'Subnet',
            CidrBlock='10.0.0.0/24',
            VpcId=Ref(self.vpc()),
            Tags=Tags(Application=Ref('AWS::StackId')))

    @memoize
    def gateway(self):
        return ec2.InternetGateway('InternetGateway')

    @memoize
    def gateway_attachment(self):
        return ec2.VPCGatewayAttachment(
            'AttachGateway',
            VpcId=Ref(self.vpc()),
            InternetGatewayId=Ref(self.gateway()))

    def route_table(self):
        return ec2.RouteTable(
            'RouteTable',
            VpcId=Ref(self.vpc()))

    def internet_route(self):
        return ec2.Route(
            'Route',
            DependsOn='AttachGateway',
            GatewayId=Ref(self.gateway()),
            DestinationCidrBlock='0.0.0.0/0',
            RouteTableId=Ref(self.route_table()))

    def subnet_route_table_association(self):
        return ec2.SubnetRouteTableAssociation(
            'SubnetRouteTableAssociation',
            SubnetId=Ref(self.subnet()),
            RouteTableId=Ref(self.route_table()))

    @memoize
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
            VpcId=Ref(self.vpc()),
        )

    @memoize
    def instance_security_group_ingress(self):
        return ec2.SecurityGroupIngress(
            "PilosaIngress",
            IpProtocol='tcp',
            FromPort='15000',
            ToPort='15000',
            GroupId=Ref(self.instance_security_group()),
            SourceSecurityGroupId=Ref(self.instance_security_group()),
        )

    @memoize
    def instance(self, index):
        return ec2.Instance(
            'PilosaInstance{}'.format(index),
            ImageId = Ref(self.ami()), #ubuntu
            InstanceType = Ref(self.instance_type()),
            KeyName = Ref(self.keypair()),
            IamInstanceProfile=Ref(self.instance_profile()),
            NetworkInterfaces=[
                ec2.NetworkInterfaceProperty(
                    GroupSet=[Ref(self.instance_security_group().title)],
                    AssociatePublicIpAddress='true',
                    DeviceIndex='0',
                    DeleteOnTermination='true',
                    SubnetId=Ref(self.subnet()))],

            UserData = Base64(dedent('''
                #!/bin/bash
                echo "Now we download Pilosa!!!"
                # TODO: download pilosa binary, run it
            '''[1:])), # strip leading newline and dedent
        )


    @memoize
    def ami(self):
        return self.template().add_parameter(Parameter(
            'AMI',
            Description='AMI to use for pilosa instance',
            Type='String',
            Default='ami-e3c3b8f4',
        ))

    @memoize
    def keypair(self):
        return self.template().add_parameter(Parameter(
            'Keypair',
            Description='Keypair to use for sudoer user',
            Type='String',
            Default='cody@soyland.org',
        ))

    @memoize
    def instance_type(self):
        return self.template().add_parameter(Parameter(
            'InstanceType',
            Description='Instance type of pilosa',
            Type='String',
            Default='m4.large',
        ))

    def generate(self):
        template = self.template()
        for resource in (self.role(),
                         self.vpc(),
                         self.hosted_zone(),
                         self.subnet(),
                         self.gateway(),
                         self.gateway_attachment(),
                         self.route_table(),
                         self.internet_route(),
                         self.subnet_route_table_association(),
                         self.record_set(0),
                         self.record_set(1),
                         self.record_set(2),
                         self.instance(0),
                         self.instance(1),
                         self.instance(2),
                         self.instance_profile(),
                         self.instance_security_group(),
                         self.instance_security_group_ingress(),
                         ):
            template.add_resource(resource)

        return template

def main():
    print PilosaTemplate().generate().to_json()

if __name__ == '__main__':
    main()
