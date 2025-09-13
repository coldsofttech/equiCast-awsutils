import os
from dataclasses import dataclass
from typing import Optional

import boto3
from botocore.exceptions import ClientError


@dataclass
class CodeArtifact:
    domain: str
    owner: str
    region_name: str
    session: Optional[boto3.Session] = None

    _repo_formats = [
        "cargo", "generic", "maven", "npm", "nuget", "pypi", "ruby", "swift"
    ]

    def __post_init__(self):
        if not self.session:
            self.session = boto3.Session(region_name=self.region_name)

        self.client = self.session.client("codeartifact", region_name=self.region_name)

    def get_auth_token(self, github_key: str = None):
        try:
            response = self.client.get_authorization_token(domain=self.domain, domainOwner=self.owner)
            token = response.get("authorizationToken")
            gh_output = os.environ.get("GITHUB_OUTPUT")
            if github_key:
                if gh_output:
                    with open(gh_output, "a", encoding="utf-8") as f:
                        f.write(f"{github_key}={token}\n")

            return {"token": token}
        except ClientError as e:
            print(f"Error fetching auth token: {e}")
            return {"error": str(e)}

    def get_endpoint(self, repo_name: str, fmt: str = "pypi", github_key: str = None):
        fmt = fmt.lower()
        if fmt not in self._repo_formats:
            raise ValueError(f"Unsupported format: {fmt}")

        try:
            response = self.client.get_repository_endpoint(
                domain=self.domain,
                domainOwner=self.owner,
                repository=repo_name,
                format=fmt
            )
            endpoint = response.get("repositoryEndpoint")

            if github_key:
                gh_output = os.environ.get("GITHUB_OUTPUT")
                if gh_output:
                    with open(gh_output, "a", encoding="utf-8") as f:
                        f.write(f"{github_key}={endpoint}\n")

            return {"endpoint": endpoint}
        except ClientError as e:
            print(f"Error fetching endpoint for {repo_name}: {e}")
            return {"error": str(e)}

    def delete_package_versions(self, pkg_name: str, repo_name: str, fmt: str = "pypi"):
        fmt = fmt.lower()
        if fmt not in self._repo_formats:
            raise ValueError(f"Unsupported format: {fmt}")

        try:
            response = self.client.list_package_versions(
                domain=self.domain,
                domainOwner=self.owner,
                repository=repo_name,
                format=fmt,
                package=pkg_name
            )
            versions = [v["version"] for v in response.get("versions", [])]

            if not versions:
                print(f"No existing versions found for package '{pkg_name}'.")
                return True

            failed = []
            for version in versions:
                try:
                    self.client.delete_package_versions(
                        domain=self.domain,
                        domainOwner=self.owner,
                        repository=repo_name,
                        format=fmt,
                        package=pkg_name,
                        versions=[version]
                    )
                    print(f"Deleted version: {version}")
                except ClientError as e:
                    print(f"Error deleting version {version}: {e}")
                    failed.append(version)

            if failed:
                print(f"Failed to delete versions: {failed}")
                return False

            return True
        except ClientError as e:
            print(f"Error listing package versions for {pkg_name}: {e}")
            return False
