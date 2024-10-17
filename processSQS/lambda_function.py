import boto3
import json
from requests.auth import HTTPBasicAuth
import requests
import os

sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')

# Configure your resources
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/039612886374/restaurant-slot-info'  # SQS Queue URL
DYNAMODB_TABLE = 'yelp-restaurants'  # DynamoDB table name
SENDER_EMAIL = 'singhdivyansh98@gmail.com'  # Verified sender email in SES


def send_email(recipient, subject, body):
    try:
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={
                'ToAddresses': [recipient]
            },
            Message={
                'Subject': {
                    'Data': subject
                },
                'Body': {
                    'Text': {
                        'Data': body
                    }
                }
            }
        )
        return response
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return None

def get_restaurant(businessId):
    if not businessId:
        return None

    # Fetch additional details from DynamoDB
    table = dynamodb.Table(DYNAMODB_TABLE)
    response = table.get_item(Key={'businessId': businessId})

    if 'Item' in response:
        return response['Item']
    return None


def lambda_handler(event, lambda_context):
    # Initialize SQS client
    sqs = boto3.client('sqs')

    # Fetch messages from the SQS queue
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=10  # Adjust the number of messages to fetch
    )

    # Check if messages are available
    if 'Messages' in response:
        messages = response['Messages']
        for message in messages:
            # Process each message
            process_message(message)
            # Delete the message from the queue after processing
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
    else:
        print("No messages available")


def process_message(record):
    print(f"Processing record: {record}")  # Log message ID for tracing
    message_body = json.loads(record['Body'])
    intent_name = message_body.get('intentName')
    slots = message_body.get('slots', {})

    if intent_name == "DiningSuggestionsIntent":
        cuisine = slots.get('cuisine')
        email = slots.get('email')

        # Get a random restaurant recommendation
        restaurantId = fetch_restaurant_ids_from_elasticsearch(cuisine)
        print(restaurantId)
        restaurant = get_restaurant(restaurantId)
        if restaurant:
            subject = f"Your {cuisine.capitalize()} Restaurant Recommendation!"
            body = (
                f"Here is your recommended restaurant:\n\n"
                f"Name: {restaurant['name']}\n"
                f"Address: {restaurant['address']}\n"
                f"Rating: {restaurant['rating']}\n"
                f"Number of Reviews: {restaurant['numberOfReviews']}\n"
            )

            # Send email with the recommendation
            send_email(email, subject, body)
            print(f"Email sent to {email} with recommendation for {restaurant['name']}.")
        else:
            print(f"No restaurant found for cuisine: {cuisine}")

    else:
        print(f"Intent not supported: {intent_name}")

def fetch_restaurant_ids_from_elasticsearch(cuisine):
    es_url = "https://search-dining-concierge-2xd7mrwsnmy5qar4cmz5fttpke.us-east-1.es.amazonaws.com"
    es_username = os.getenv('ELASTICSEARCH_USERNAME')
    es_password = os.getenv('ELASTICSEARCH_PASSWORD')

    response = requests.get(f"{es_url}/restaurants/_search", json={
        "query": {
            "function_score": {
                "query": {
                    "match": {
                        "Cuisine": cuisine
                    }
                },
                "random_score": {}
            }
        },
        "size": 1  # Limit the result to 1 document
        },
        auth=HTTPBasicAuth(es_username, es_password)
    )
    print(f"response from opensearch: {response.json()}")


    if response.status_code == 200:
        hits = response.json().get('hits', {}).get('hits', [])
        print(hits)
        return hits[0]['_source']['RestaurantID']
    return None
