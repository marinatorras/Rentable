import boto3
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import time
from datetime import datetime
import uuid
from decimal import Decimal, getcontext, ROUND_HALF_UP

# Set up a more permissive Decimal context
getcontext().prec = 10  # Set precision
getcontext().rounding = ROUND_HALF_UP  # Ensure rounding works properly

# Convert float to Decimal safely
def safe_decimal(value):
    """Convert a float to Decimal, rounding where necessary"""
    return Decimal(value).quantize(Decimal('0.01'))  # Rounded to 2 decimal places


# Weather fetching function
def get_weather_data(city):
    base_url = 'https://api.weather.gov/points'
    
    lat_lon_map = {
        'Madison': '43.0731,-89.4012'  # Example lat/lon for Madison, WI
    }
    
    lat_lon = lat_lon_map.get(city)
    if lat_lon is None:
        return None
    
    response = requests.get(f'{base_url}/{lat_lon}')
    
    if response.status_code == 200:
        data = response.json()
        forecast_url = data['properties']['forecast']
        
        forecast_response = requests.get(forecast_url)
        if forecast_response.status_code == 200:
            forecast_data = forecast_response.json()
            return forecast_data['properties']['periods'][0]  # Return current forecast
    return None

# Update weather data for each property
def update_weather_data_for_property(property_id, city, table):
    weather_data = get_weather_data(city)
    
    if weather_data:
        table.update_item(
            Key={'property_id': property_id},
            UpdateExpression="set weather = :w",
            ExpressionAttributeValues={
                ':w': weather_data
            }
        )
        print(f"Updated weather for property {property_id}")
    else:
        print(f"Failed to fetch weather data for {property_id} in {city}")


# Background job to fetch weather data for all properties and generate statistics
def fetch_weather_data_for_all_properties(dynamodb):
    start_time = datetime.now()  # Start time of the run
    table = dynamodb.Table('Properties')
    stats_table = dynamodb.Table('RunStatistics')

    success_count = 0
    failure_count = 0
    start_timestamp = datetime.now().isoformat()

    response = table.scan()

    for item in response['Items']:
        property_id = item['property_id']
        city = 'Madison'  # Assuming city is always Madison for now

        try:
            update_weather_data_for_property(property_id, city, table)
            success_count += 1
        except Exception as e:
            print(f"Failed to update weather for {property_id}: {e}")
            failure_count += 1

    # Calculate the time taken
    end_time = datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()

    # Generate unique run ID
    run_id = str(uuid.uuid4())

    # Insert statistics into the statistics table
    stats_table.put_item(
        Item={
            'run_id': run_id,
            'start_time': start_timestamp,
            'end_time': end_time.isoformat(),
            'success_count': safe_decimal(success_count),
            'failure_count': safe_decimal(failure_count),
            'elapsed_time_seconds': safe_decimal(elapsed_time)
        }
    )
    print(f"Run statistics: {success_count} successes, {failure_count} failures, {elapsed_time:.2f} seconds elapsed.")


# Start the background scheduler
def start_scheduler(db):
    scheduler = BackgroundScheduler()
    fetch_weather_data_for_all_properties(db)
    # Use lambda to defer the function call and pass db as an argument
    scheduler.add_job(lambda: fetch_weather_data_for_all_properties(db), 'interval', minutes=1)
    scheduler.start()

    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

