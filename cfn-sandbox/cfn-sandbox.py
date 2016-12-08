from troposphere_sugar import Skel
from troposphere_sugar.decorators import cfparam, cfresource
from troposphere import route53

class SandboxTemplate(Skel):
    @cfresource
    def hosted_zone(self):
        return route53.HostedZone(
            'SandboxPublicHostedZone',
            Name='sandbox.pilosa.com',
        )

def main():
    print SandboxTemplate().output

if __name__ == '__main__':
    main()
