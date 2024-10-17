import requests
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
import os

opensearch_endpoint = "https://search-dining-concierge-2xd7mrwsnmy5qar4cmz5fttpke.us-east-1.es.amazonaws.com"

# Master user credentials
master_username = os.getenv('ELASTICSEARCH_USERNAME')
master_password = os.getenv('ELASTICSEARCH_PASSWORD')

def index_restaurant(restaurant_id, cuisine):
    document = {
        "RestaurantID": restaurant_id,
        "Cuisine": cuisine
    }

    response = requests.put(
        f"{opensearch_endpoint}/restaurants/_doc/{restaurant_id}",
        data=json.dumps(document),
        auth=HTTPBasicAuth(master_username, master_password),
        headers={"Content-Type": "application/json"}
    )

    if response.status_code in [200, 201]:
        print(f"Successfully indexed restaurant: {restaurant_id}")
    else:
        print(f"Failed to index restaurant: {response.status_code} - {response.content.decode()}")

# Load the CSV data
csv_file_path = 'restaurants.csv'
restaurants_df = pd.read_csv(csv_file_path)

# Iterate through the rows of the DataFrame and index each restaurant
for _, row in restaurants_df.iterrows():
    index_restaurant(row["Restaurant ID"], row["Cuisine"])
