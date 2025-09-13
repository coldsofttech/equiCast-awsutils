import tempfile
from unittest.mock import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

from equicast_awsutils import CodeArtifact


@pytest.fixture
def mock_client():
    with patch("boto3.Session.client") as mock_client:
        yield mock_client


def make_client_with_versions(versions, fail_delete=None):
    mock_instance = MagicMock()
    mock_instance.list_package_versions.return_value = {
        "versions": [{"version": v} for v in versions]
    }

    def delete_side_effect(**kwargs):
        v = kwargs["versions"][0]
        if fail_delete and v in fail_delete:
            raise ClientError(
                {"Error": {"Code": "BadRequest", "Message": f"Failed to delete {v}"}},
                "DeletePackageVersions"
            )
        return {"successfulVersions": [v]}

    mock_instance.delete_package_versions.side_effect = delete_side_effect
    return mock_instance


def test_get_auth_token_no_github_key(mock_client):
    mock_instance = MagicMock()
    mock_instance.get_authorization_token.return_value = {"authorizationToken": "FAKE_TOKEN"}
    mock_client.return_value = mock_instance

    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
    result = ca.get_auth_token()

    assert result["token"] == "FAKE_TOKEN"
    mock_instance.get_authorization_token.assert_called_once_with(
        domain="test-domain", domainOwner="123456789012"
    )


def test_get_auth_token_with_github_key(mock_client, monkeypatch):
    mock_instance = MagicMock()
    mock_instance.get_authorization_token.return_value = {"authorizationToken": "FAKE_TOKEN"}
    mock_client.return_value = mock_instance

    with tempfile.NamedTemporaryFile(delete=False, mode="w+") as tmpfile:
        monkeypatch.setenv("GITHUB_OUTPUT", tmpfile.name)
        ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
        result = ca.get_auth_token(github_key="CODEARTIFACT_TOKEN")

        assert result["token"] == "FAKE_TOKEN"

    with open(tmpfile.name, "r", encoding="utf-8") as f:
        content = f.read()
        assert "CODEARTIFACT_TOKEN=FAKE_TOKEN" in content


def test_get_auth_token_client_error(mock_client):
    mock_instance = MagicMock()
    mock_instance.get_authorization_token.side_effect = ClientError(
        {"Error": {"Code": "BadRequest", "Message": "Invalid request"}}, "GetAuthorizationToken"
    )
    mock_client.return_value = mock_instance

    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
    result = ca.get_auth_token()

    assert "error" in result
    assert "BadRequest" in result["error"]


def test_get_endpoint_no_github_key(mock_client):
    mock_instance = MagicMock()
    mock_instance.get_repository_endpoint.return_value = {"repositoryEndpoint": "https://fake.endpoint"}
    mock_client.return_value = mock_instance

    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
    result = ca.get_endpoint("my-repo", fmt="pypi")

    assert result["endpoint"] == "https://fake.endpoint"
    mock_instance.get_repository_endpoint.assert_called_once_with(
        domain="test-domain", domainOwner="123456789012", repository="my-repo", format="pypi"
    )


def test_get_endpoint_with_github_key(mock_client, monkeypatch):
    mock_instance = MagicMock()
    mock_instance.get_repository_endpoint.return_value = {"repositoryEndpoint": "https://fake.endpoint"}
    mock_client.return_value = mock_instance

    with tempfile.NamedTemporaryFile(delete=False, mode="w+") as tmpfile:
        monkeypatch.setenv("GITHUB_OUTPUT", tmpfile.name)
        ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
        result = ca.get_endpoint("my-repo", fmt="pypi", github_key="CODEARTIFACT_ENDPOINT")

        assert result["endpoint"] == "https://fake.endpoint"

    with open(tmpfile.name, "r", encoding="utf-8") as f:
        content = f.read()
        assert "CODEARTIFACT_ENDPOINT=https://fake.endpoint" in content


def test_get_endpoint_invalid_format(mock_client):
    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
    with pytest.raises(ValueError):
        ca.get_endpoint("my-repo", fmt="invalidfmt")


def test_get_endpoint_client_error(mock_client):
    mock_instance = MagicMock()
    mock_instance.get_repository_endpoint.side_effect = ClientError(
        {"Error": {"Code": "BadRequest", "Message": "Invalid request"}}, "GetRepositoryEndpoint"
    )
    mock_client.return_value = mock_instance

    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
    result = ca.get_endpoint("my-repo", fmt="pypi")

    assert "error" in result
    assert "BadRequest" in result["error"]


def test_delete_package_versions_multiple_success(mock_client):
    mock_client.return_value = make_client_with_versions(["1.0.0", "1.1.0"])

    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
    result = ca.delete_package_versions("mypkg", "myrepo")

    assert result is True
    assert mock_client.return_value.list_package_versions.called
    assert mock_client.return_value.delete_package_versions.call_count == 2


def test_delete_package_versions_single_success(mock_client):
    mock_client.return_value = make_client_with_versions(["1.0.0"])

    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
    result = ca.delete_package_versions("mypkg", "myrepo")

    assert result is True
    mock_client.return_value.delete_package_versions.assert_called_once()


def test_delete_package_versions_single_fail(mock_client):
    mock_client.return_value = make_client_with_versions(["1.0.0"], fail_delete={"1.0.0"})

    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
    result = ca.delete_package_versions("mypkg", "myrepo")

    assert result is False


def test_delete_package_versions_multiple_partial_fail(mock_client):
    mock_client.return_value = make_client_with_versions(
        ["1.0.0", "1.1.0", "2.0.0"], fail_delete={"1.1.0"}
    )

    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
    result = ca.delete_package_versions("mypkg", "myrepo")

    assert result is False
    assert mock_client.return_value.delete_package_versions.call_count == 3


def test_delete_package_versions_none_found(mock_client):
    mock_instance = MagicMock()
    mock_instance.list_package_versions.return_value = {"versions": []}
    mock_client.return_value = mock_instance

    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")
    result = ca.delete_package_versions("mypkg", "myrepo")

    assert result is True
    mock_instance.delete_package_versions.assert_not_called()


def test_delete_package_versions_invalid_format(mock_client):
    ca = CodeArtifact(domain="test-domain", owner="123456789012", region_name="eu-west-1")

    with pytest.raises(ValueError):
        ca.delete_package_versions("mypkg", "myrepo", fmt="invalidfmt")
