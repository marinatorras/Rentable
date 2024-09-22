import xml.etree.ElementTree as ET
import boto3
# from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Initialize DynamoDB connection (local setup)
def init_dynamodb():
    dynamodb = boto3.resource(
        'dynamodb',
        endpoint_url='http://localhost:8000',  # Connecting to local DynamoDB
        region_name='us-west-2',
        aws_access_key_id='lalala',   # DynamoDB Local doesn't require real credentials
        aws_secret_access_key='lalala'
    )
    return dynamodb

# Create a DynamoDB table for the property properties
def create_properties_table(dynamodb):
    table_name = 'Properties'
    
    existing_tables = dynamodb.tables.all()
    if any(table.name == table_name for table in existing_tables):
        return dynamodb.Table(table_name)
    
    table = dynamodb.create_properties_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'property_id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'property_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    return table


# Create a DynamoDB table for the stats of the fetching the weather job
def create_statistics_table(dynamodb):
    table_name = 'RunStatistics'

    existing_tables = [table.name for table in dynamodb.tables.all()]
    if table_name in existing_tables:
        return dynamodb.Table(table_name)

    # Create table for statistics
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'run_id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'run_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    return table


# Save data into the DynamoDB table
def save_to_dynamodb(table, properties):
    for prop in properties:
        table.put_item(
            Item={
                'property_id': prop['property_id'],
                'name': prop['name'],
                'email': prop['email'],
                'num_bedrooms': prop['num_bedrooms']
            }
        )
    print(f"Saved {len(properties)} items to DynamoDB.")

