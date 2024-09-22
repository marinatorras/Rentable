# Rentable Project

This Python application parses property data from an XML file, saves it to a local DynamoDB instance, and periodically fetches weather data for each property using the National Weather Service API. The application also tracks statistics for each weather update job and stores them in a separate DynamoDB table.

## Prerequisites
Before running this program, ensure you have the following tools installed:

- Python 3.7+
- Docker: Docker is required to run DynamoDB locally
- AWS CLI

You will also need the following Python libraries installed:
- boto3: For interacting with AWS DynamoDB.
- requests: For fetching data from the weather API.
- apscheduler: For scheduling recurring tasks.

## Set Up Docker for DynamoDB Local

DynamoDB will be used to store both property data and job statistics. Start DynamoDB Local using Docker:

```python
docker pull amazon/dynamodb-local
docker run -d -p 8000:8000 amazon/dynamodb-local
```

## Running the application

```python
python rentable_main.py
```

This will:

1. Parse the property data from the XML file (Property ID, name, and email, and only include properties in Madison).
2. Fetch the weather data for each property.
3. Save the XML file data plus the weather data to a DynamoDB table named Properties.
4. Schedule a background job to fetch and update weather data every 1 minue.
5. Store run statistics in the RunStatistics table.

## Checking the contents of the DB

To inspect the DynamoDB Properties table for Property ID, name, email, and weather data:

```python
aws dynamodb scan --table-name Properties --endpoint-url http://localhost:8000
```

To inspect the DynamoDB RunStatistics table for each job’s performance (success/failure counts, job duration):

```python
aws dynamodb scan --table-name RunStatistics --endpoint-url http://localhost:8000
```