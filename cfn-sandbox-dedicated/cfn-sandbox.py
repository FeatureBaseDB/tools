import sys

from troposphere import route53, iam, ec2, Ref, Template, GetAtt, Base64, Parameter, Tags, Sub
from troposphere_sugar import Skel
from troposphere_sugar.decorators import cfparam, cfresource
from troposphere import route53

class SandboxTemplate(Skel):
    def __init__(self, domain):
        self.domain = domain

    @cfresource
    def hosted_zone(self):
        return route53.HostedZone(
            'SandboxPublicHostedZone',
            Name=self.domain,
        )

    @cfresource
    def vpc(self):
        return ec2.VPC(
            'DedicatedVPC',
            InstanceTenancy='dedicated',
            CidrBlock='10.0.0.0/16',
            EnableDnsHostnames='true',
            EnableDnsSupport='true',
        )

    @cfresource
    def subnet_b(self):
        return ec2.Subnet(
            'DedicatedSubnetB',
            CidrBlock='10.0.1.0/24',
            VpcId=Ref(self.vpc))

    @cfresource
    def subnet_c(self):
        return ec2.Subnet(
            'DedicatedSubnetC',
            CidrBlock='10.0.2.0/24',
            VpcId=Ref(self.vpc))

    @cfresource
    def subnet_d(self):
        return ec2.Subnet(
            'DedicatedSubnetD',
            CidrBlock='10.0.3.0/24',
            VpcId=Ref(self.vpc))

    @cfresource
    def subnet_e(self):
        return ec2.Subnet(
            'DedicatedSubnetE',
            CidrBlock='10.0.4.0/24',
            VpcId=Ref(self.vpc))

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
    def subnet_route_table_association_b(self):
        return ec2.SubnetRouteTableAssociation(
            'SubnetRouteTableAssociationB',
            SubnetId=Ref(self.subnet_b),
            RouteTableId=Ref(self.route_table))

    @cfresource
    def subnet_route_table_association_c(self):
        return ec2.SubnetRouteTableAssociation(
            'SubnetRouteTableAssociationC',
            SubnetId=Ref(self.subnet_c),
            RouteTableId=Ref(self.route_table))

    @cfresource
    def subnet_route_table_association_d(self):
        return ec2.SubnetRouteTableAssociation(
            'SubnetRouteTableAssociationD',
            SubnetId=Ref(self.subnet_d),
            RouteTableId=Ref(self.route_table))

    @cfresource
    def subnet_route_table_association_e(self):
        return ec2.SubnetRouteTableAssociation(
            'SubnetRouteTableAssociationE',
            SubnetId=Ref(self.subnet_e),
            RouteTableId=Ref(self.route_table))

def main():
    domain = "sandbox-dedicated.pilosa.com"
    if len(sys.argv) > 1:
        domain = sys.argv[1]

    print SandboxTemplate(domain=domain).output

if __name__ == '__main__':
    main()
