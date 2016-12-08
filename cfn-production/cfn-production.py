from troposphere_sugar import Skel
from troposphere_sugar.decorators import cfparam, cfresource
from troposphere import route53, Ref, Parameter, Join

class ProductionTemplate(Skel):
    @cfparam
    def hosted_zone_dns_name(self):
        return Parameter(
            "HostedZoneName",
            Description="The DNS name of an existing Amazon Route 53 hosted zone",
            Type="String",
            Default="pilosa.com",
        )

    @property
    def hosted_zone_name(self):
        """
        Adds dot to end of hosted zone dns name, ie "example.com" -> "example.com."
        """
        return Join('', [Ref(self.hosted_zone_dns_name), '.'])

    @cfresource
    def hosted_zone(self):
        return route53.HostedZone(
            'HostedZone',
            Name=self.hosted_zone_name,
        )

    @cfresource
    def sandbox_record_set(self):
        return route53.RecordSetType(
            'SandboxRecordSet',
            HostedZoneId=Ref(self.hosted_zone),
            Name=Join('', ['sandbox.', self.hosted_zone_name]),
            Type="NS",
            TTL="900",
            ResourceRecords=[
                'ns-1163.awsdns-17.org.',
                'ns-442.awsdns-55.com.',
                'ns-847.awsdns-41.net.',
                'ns-1867.awsdns-41.co.uk.',
            ],
        )

    @cfresource
    def dot_com_record_set(self):
        return route53.RecordSetType(
            'PilosaDotComRecordSet',
            HostedZoneId=Ref(self.hosted_zone),
            Name=self.hosted_zone_name,
            Type="A",
            TTL="900",
            ResourceRecords=[
                '198.185.159.144',
                '198.185.159.145',
                '198.49.23.145',
                '198.49.23.144',
            ],
        )

    @cfresource
    def mx_record_set(self):
        return route53.RecordSetType(
            'MXRecordSet',
            HostedZoneId=Ref(self.hosted_zone),
            Name=self.hosted_zone_name,
            Type="MX",
            TTL="86400",
            ResourceRecords=[
                '1 ASPMX.L.GOOGLE.COM.',
                '5 ALT1.ASPMX.L.GOOGLE.COM.',
                '5 ALT2.ASPMX.L.GOOGLE.COM.',
                '10 ALT3.ASPMX.L.GOOGLE.COM.',
                '10 ALT4.ASPMX.L.GOOGLE.COM.',
            ],
        )

    @cfresource
    def txt_record_set(self):
        return route53.RecordSetType(
            'TXTRecordSet',
            HostedZoneId=Ref(self.hosted_zone),
            Name=self.hosted_zone_name,
            Type="TXT",
            TTL="86400",
            ResourceRecords=[
                '"google-site-verification=TzJvGzIQZJWwPSkSMxlVh0mMKiCsdwkVXX8Q_g_XzuI"',
            ],
        )

    @cfresource
    def squarespace_verify_record_set(self):
        return route53.RecordSetType(
            'SquarespaceVerifyRecordSet',
            HostedZoneId=Ref(self.hosted_zone),
            Name=Join('', ['l9gnfpg7g8edla25773h.', self.hosted_zone_name]),
            Type="CNAME",
            TTL="900",
            ResourceRecords=[
                'verify.squarespace.com.',
            ],
        )

    @cfresource
    def www_record_set(self):
        return route53.RecordSetType(
            'WWWRecordSet',
            HostedZoneId=Ref(self.hosted_zone),
            Name=Join('', ['www.', self.hosted_zone_name]),
            Type="CNAME",
            TTL="900",
            ResourceRecords=[
                'ext-cust.squarespace.com.',
            ],
        )


def main():
    print ProductionTemplate().output

if __name__ == '__main__':
    main()
