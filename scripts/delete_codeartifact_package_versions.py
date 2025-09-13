import argparse

from equicast_awsutils import CodeArtifact


def main():
    parser = argparse.ArgumentParser(description="Delete all versions of a package from AWS CodeArtifact")
    parser.add_argument("--domain", required=True, help="CodeArtifact domain")
    parser.add_argument("--domain-owner", required=True, help="AWS account ID of domain owner")
    parser.add_argument("--repository", required=True, help="CodeArtifact repository")
    parser.add_argument("--package-name", required=True, help="Package name")
    parser.add_argument("--format", default="pypi", help="Package format (default: pypi)")
    args = parser.parse_args()

    ca = CodeArtifact(domain=args.domain, owner=args.domain_owner, region_name="eu-west-1")
    ca.delete_package_versions(pkg_name=args.package_name, repo_name=args.repository, fmt=args.format)


if __name__ == '__main__':
    main()
