import argparse

from equicast_awsutils import SSM


def main():
    parser = argparse.ArgumentParser(description="Store a parameter in AWS SSM Parameter Store")
    parser.add_argument("--name", required=True, help="SSM parameter name, e.g. /equicast/pyutils/version")
    parser.add_argument("--value", required=True, help="Value to store")
    parser.add_argument("--type", default="String", choices=["String", "StringList", "SecureString"],
                        help="Parameter type")
    parser.add_argument("--region", default="eu-west-1", help="AWS region")
    parser.add_argument("--no-overwrite", action="store_true", help="Do not overwrite if parameter exists")
    args = parser.parse_args()

    ssm = SSM(region_name=args.region)
    ssm.update_parameter(param_name=args.name, value=args.value, type_=args.type, overwrite=not args.no_overwrite)


if __name__ == "__main__":
    main()
