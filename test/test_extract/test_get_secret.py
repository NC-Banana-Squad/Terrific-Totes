import boto3
import json
from moto import mock_aws
from util_functions import get_secret
from unittest.mock import patch
from botocore.exceptions import ClientError
import pytest


@mock_aws
def test_get_secret():

    mock_secret_name = "test-database-secret"
    region_name = "eu-west-2"
    mock_secret_values = {
        "user": "test-user",
        "database": "test-database",
        "password": "test-password",
        "host": "localhost",
        "port": "5432",
    }

    mock_client = boto3.client("secretsmanager", region_name=region_name)
    mock_client.create_secret(
        Name=mock_secret_name, SecretString=json.dumps(mock_secret_values)
    )

    result = get_secret(secret_name=mock_secret_name)

    assert result == mock_secret_values


@mock_aws
def test_get_secret_missing_secret():

    secret_name = "nonexistent-secret"
    region_name = "us-east-1"

    with patch("boto3.client") as mock_boto_client:
        mock_client = mock_boto_client.return_value

        mock_client.get_secret_value.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "Secret not found",
                }
            },
            operation_name="GetSecretValue",
        )

        with pytest.raises(RuntimeError, match="Failed to retrieve secret:"):
            get_secret(secret_name, region_name=region_name)
