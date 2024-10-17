import boto3
import requests
import datetime
import time
from decimal import Decimal
from collections import defaultdict
import csv
import os
from dotenv import load_dotenv


load_dotenv()
# Load keys from .env file
yelp_api_key = os.getenv('YELP_API_KEY')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Define cuisine types with limits
cuisine_limits = {
    'mexican': 20,
    'chinese': 20,
    'italian': 10
}

# Configure AWS credentials and region
dynamodb = boto3.resource(
    'dynamodb',
    region_name='us-east-1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

table = dynamodb.Table('yelp-restaurants')

def get_yelp_restaurants(cuisine, offset=0):
    url = f"https://api.yelp.com/v3/businesses/search?term={cuisine}&location=Manhattan&limit=50&offset={offset}"
    headers = {'Authorization': f'Bearer {yelp_api_key}'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data.get('businesses', []), data.get('total', 0)
    else:
        print(f"Error fetching data from Yelp API: {response.status_code} - {response.text}")
        return [], 0

# Function to batch insert restaurants into DynamoDB
def batch_insert_into_dynamodb(restaurants):
    with table.batch_writer() as batch:
        for restaurant in restaurants:
            try:
                item = {
                    'insertedAtTimestamp': str(datetime.datetime.now()),
                    'businessId': restaurant['id'],
                    'name': restaurant['name'],
                    'address': restaurant['location']['address1'],
                    'coordinates': {
                        'latitude': Decimal(str(restaurant['coordinates']['latitude'])),
                        'longitude': Decimal(str(restaurant['coordinates']['longitude']))
                    },
                    'numberOfReviews': restaurant['review_count'],
                    'rating': Decimal(str(restaurant['rating'])),
                    'zipCode': restaurant['location']['zip_code']
                }
                batch.put_item(Item=item)
                print(f"Queued {restaurant['name']} for insertion.")
            except Exception as e:
                print(f"Error inserting {restaurant['name']} into DynamoDB: {e}")

def write_to_csv(elastic_map, filename="restaurants.csv"):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Cuisine', 'Restaurant ID']) 
        for cuisine, restaurant_ids in elastic_map.items():
            for restaurant_id in restaurant_ids:
                writer.writerow([cuisine, restaurant_id])
    print(f"Data successfully written to {filename}")

# Main loop for scraping and storing data
elastic_map = defaultdict(list)  # Dictionary to store cuisine and restaurant IDs

for cuisine, limit in cuisine_limits.items():
    offset = 0
    total_restaurants_fetched = 0

    while total_restaurants_fetched < limit:
        print(f"Fetching {cuisine} restaurants (offset {offset})...")
        restaurants, total = get_yelp_restaurants(cuisine, offset)

        if not restaurants:
            print(f"No more {cuisine} restaurants found.")
            break

        # Limit to the number of restaurants specified for this cuisine
        if total_restaurants_fetched + len(restaurants) > limit:
            restaurants = restaurants[:limit - total_restaurants_fetched]

        # Add fetched restaurant IDs to the elastic_map
        for res in restaurants:
            elastic_map[cuisine].append(res["id"])

        batch_insert_into_dynamodb(restaurants)
        total_restaurants_fetched += len(restaurants)


        # Stop if we've reached the maximum allowed offset
        if total_restaurants_fetched >= limit:
            print(f"Reached maximum or all {cuisine} restaurants fetched.")
            break

        # Sleep to avoid hitting Yelp API rate limits
        time.sleep(1)

# Write results to CSV
write_to_csv(elastic_map)
print("Data fetching complete.")
