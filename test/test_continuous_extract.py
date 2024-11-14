from src.extract import continuous_extract
from moto import mock_aws
import boto3
import pytest

from unittest.mock import Mock, patch, call
import io
from botocore.exceptions import ClientError

@pytest.fixture
def mock_dependencies():
    with patch('src.extract.create_s3_client') as mock_create_s3_client, \
         patch('src.extract.connect') as mock_connect, \
         patch('src.extract.create_file_name') as mock_create_file_name, \
         patch('src.extract.format_to_csv') as mock_format_to_csv, \
         patch('src.extract.store_in_s3') as mock_store_in_s3:
        
        # Setup s3 client mock
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            'Body': Mock(read=lambda: b'2024-01-01')
        }
        mock_create_s3_client.return_value = mock_s3
        
        # Setup database connection mock
        mock_db = Mock()
        mock_db.columns = [{'name': 'id'}, {'name': 'created_at'}]
        mock_connect.return_value = mock_db
        
        # Setup other mocks
        mock_create_file_name.side_effect = lambda table: f"{table}_export.csv"
        mock_format_to_csv.return_value = io.StringIO("id,created_at\n1,2024-01-02")
        
        yield {
            's3_client': mock_s3,
            'db_conn': mock_db,
            'create_file_name': mock_create_file_name,
            'format_to_csv': mock_format_to_csv,
            'store_in_s3': mock_store_in_s3
        }

def test_continuous_extract_successful_flow(mock_dependencies):
    # Setup test data
    tables = [{'table_name': 'users'}, {'table_name': 'orders'}]
    mock_dependencies['db_conn'].run.side_effect = [
        tables,  # First call returns tables
        [{'id': 1, 'created_at': '2024-01-02'}],  # Second call returns users data
        [{'id': 2, 'created_at': '2024-01-02'}]   # Third call returns orders data
    ]
    
    # Execute function
    result = continuous_extract()
    
    # Verify results
    assert result == {"result": "Success"}
    
    # Verify S3 interactions
    mock_dependencies['s3_client'].get_object.assert_called_once_with(
        Bucket='banana-squad-code',
        Key='last_extracted.txt'
    )
    
    # Verify database queries
    expected_db_calls = [
        call('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_name != \'_prisma_migrations\''),
        call('SELECT * FROM users WHERE created_at > 2024-01-01'),
        call('SELECT * FROM orders WHERE created_at > 2024-01-01')
    ]
    assert mock_dependencies['db_conn'].run.call_args_list == expected_db_calls
    
    # Verify file creation and storage
    assert mock_dependencies['create_file_name'].call_args_list == [
        call({'table_name': 'users'}),
        call({'table_name': 'orders'})
    ]
    
    assert mock_dependencies['format_to_csv'].call_args_list == [
        call([{'id': 1, 'created_at': '2024-01-02'}], [{'name': 'id'}, {'name': 'created_at'}]),
        call([{'id': 2, 'created_at': '2024-01-02'}], [{'name': 'id'}, {'name': 'created_at'}])
    ]
    
    assert mock_dependencies['store_in_s3'].call_args_list == [
        call(mock_dependencies['s3_client'], 
             mock_dependencies['format_to_csv'].return_value, 
             bucket_name, 
             'users_export.csv'),
        call(mock_dependencies['s3_client'], 
             mock_dependencies['format_to_csv'].return_value, 
             bucket_name, 
             'orders_export.csv')
    ]
    
    # Verify connection was closed
    mock_dependencies['db_conn'].close.assert_called_once()

def test_continuous_extract_no_new_data(mock_dependencies):
    # Setup test data with no rows returned
    tables = [{'table_name': 'users'}]
    mock_dependencies['db_conn'].run.side_effect = [
        tables,  # First call returns tables
        []      # Second call returns no rows
    ]
    
    # Execute function
    result = continuous_extract()
    
    # Verify results
    assert result == {"result": "Success"}
    
    # Verify store_in_s3 was not called
    mock_dependencies['store_in_s3'].assert_not_called()
    
    # Verify connection was closed
    mock_dependencies['db_conn'].close.assert_called_once()

def test_continuous_extract_empty_tables(mock_dependencies):
    # Setup test data with no tables
    mock_dependencies['db_conn'].run.return_value = []
    
    # Execute function
    result = continuous_extract()
    
    # Verify results
    assert result == {"result": "Success"}
    
    # Verify no further processing was done
    mock_dependencies['create_file_name'].assert_not_called()
    mock_dependencies['format_to_csv'].assert_not_called()
    mock_dependencies['store_in_s3'].assert_not_called()
    
    # Verify connection was closed
    mock_dependencies['db_conn'].close.assert_called_once()
