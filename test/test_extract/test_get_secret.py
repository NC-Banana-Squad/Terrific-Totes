import boto3
import json
from moto import mock_aws
from src.extract.util_functions import get_secret

@mock_aws
def test_get_secret():
    # Define the secret name and values
    mock_secret_name = "test-database-secret"
    mock_secret_values = {
        "user": "test-user",
        "database": "test-database",
        "password": "test-password",
        "host": "localhost",
        "port": "5432"
    }

    # Set up mock Secrets Manager
    mock_client = boto3.client("secretsmanager")
    mock_client.create_secret(
        Name=mock_secret_name,
        SecretString=json.dumps(mock_secret_values)
    )

    result = get_secret(secret_name=mock_secret_name)

    assert result == mock_secret_values

@mock_aws
def test_get_secret_missing():
    secret_name = "nonexistent-secret"

    # Call the function and check for the appropriate exception
    try:
        get_secret(secret_name)
    except Exception as e:
        assert "Secrets Manager can't find the specified secret" in str(e)