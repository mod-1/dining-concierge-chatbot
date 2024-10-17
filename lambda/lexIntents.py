import json
import boto3
import os

# Initialize SQS client
sqs = boto3.client('sqs')


SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/039612886374/restaurant-slot-info"

def lambda_handler(event, context):
    """
    Handle Lex intent fulfillment, gather slot information, and send it to SQS.
    """
    try:
        print(f"Received event: {json.dumps(event)}")

        # Get the intent name and slots from Lex input
        intent_name = event['sessionState']['intent']['name']
        slots = event['sessionState']['intent'].get('slots', {})

        # Extract relevant slot data
        slot_data = {}
        for slot_name, slot_info in slots.items():
            if slot_info and slot_info.get('value'):
                slot_data[slot_name] = slot_info['value']['interpretedValue']

        if intent_name == "DiningSuggestionsIntent":
            # Prepare the SQS message
            sqs_message = {
                'intentName': intent_name,
                'slots': slot_data
            }

            # Send the message to SQS
            response = sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(sqs_message)
            )

            # Log the response from SQS
            print(f"SQS send message response: {response}")

        # Return success response to Lex
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"
                },
                "intent": {
                    "name": intent_name,
                    "state": "Fulfilled"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "You’re all set. Expect my suggestions shortly! Have a good day."
                }
            ]
        }

    except Exception as e:
        print(f"Error processing request: {str(e)}")

        # Return an error message to Lex if something went wrong
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"
                },
                "intent": {
                    "name": event['sessionState']['intent']['name'],
                    "state": "Failed"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "There was an issue processing your request."
                }
            ]
        }
