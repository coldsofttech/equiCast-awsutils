from unittest.mock import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

from equicast_awsutils import SSM


@pytest.fixture
def mock_ssm_client():
    with patch("boto3.Session.client") as mock_client:
        yield mock_client


def test_update_parameter_success(mock_ssm_client):
    mock_instance = MagicMock()
    mock_instance.put_parameter.return_value = {"Version": 1}
    mock_ssm_client.return_value = mock_instance

    ssm = SSM(region_name="eu-west-1")
    result = ssm.update_parameter("/my/param", "value123")

    assert result is True
    mock_instance.put_parameter.assert_called_once_with(
        Name="/my/param",
        Value="value123",
        Type="String",
        Overwrite=True
    )


def test_update_parameter_invalid_type(mock_ssm_client):
    ssm = SSM(region_name="eu-west-1")
    with pytest.raises(ValueError, match="Invalid parameter type"):
        ssm.update_parameter("/my/param", "value", type_="InvalidType")


def test_update_parameter_client_error(mock_ssm_client):
    mock_instance = MagicMock()
    mock_instance.put_parameter.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "Denied"}}, "PutParameter"
    )
    mock_ssm_client.return_value = mock_instance

    ssm = SSM(region_name="eu-west-1")
    result = ssm.update_parameter("/my/param", "value123")

    assert result is False
    mock_instance.put_parameter.assert_called_once()


def test_update_parameter_overwrite_false(mock_ssm_client):
    mock_instance = MagicMock()
    mock_instance.put_parameter.return_value = {"Version": 2}
    mock_ssm_client.return_value = mock_instance

    ssm = SSM(region_name="eu-west-1")
    result = ssm.update_parameter("/my/param", "value123", overwrite=False)

    assert result is True
    mock_instance.put_parameter.assert_called_once_with(
        Name="/my/param",
        Value="value123",
        Type="String",
        Overwrite=False
    )


@pytest.mark.parametrize("param_type", ["String", "StringList", "SecureString"])
def test_update_parameter_all_types(mock_ssm_client, param_type):
    mock_instance = MagicMock()
    mock_instance.put_parameter.return_value = {"Version": 1}
    mock_ssm_client.return_value = mock_instance

    ssm = SSM(region_name="eu-west-1")
    result = ssm.update_parameter("/my/param", "value123", type_=param_type)

    assert result is True
    mock_instance.put_parameter.assert_called_once_with(
        Name="/my/param",
        Value="value123",
        Type=param_type,
        Overwrite=True
    )
