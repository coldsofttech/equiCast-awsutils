import argparse

from equicast_awsutils import CodeArtifact


def main():
    parser = argparse.ArgumentParser(description="Fetch CodeArtifact token and endpoint.")
    parser.add_argument("--domain", required=True, help="CodeArtifact domain name")
    parser.add_argument("--domain-owner", required=True, help="CodeArtifact domain owner AWS account ID")
    parser.add_argument("--repository", required=True, help="CodeArtifact repository name")
    args = parser.parse_args()

    ca = CodeArtifact(domain=args.domain, owner=args.domain_owner, region_name="eu-west-1")
    ca.get_auth_token(github_key="token")
    ca.get_endpoint(repo_name=args.repository, github_key="endpoint")


if __name__ == '__main__':
    main()
