import boto3
import json

# Create Lex V2 runtime client
lex_runtime_v2 = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    """
    Lambda handler function to process Lex V2 bot interaction.
    """
    # Extract message content from the request
    message_content = event['messages']

    # Set up parameters to send the message to Lex V2
    params = {
        'botId': '7KXPJ3VIMC',
        'botAliasId': 'EDQVWNSEOX',
        'localeId': 'en_US',
        'sessionId': 'user-id',
        'text': message_content
    }

    try:
        # Send the message to Lex V2 using the recognize_text API
        lex_response = lex_runtime_v2.recognize_text(**params)

        print(f"Intent: {lex_response['sessionState']['intent']['name']}, Slots: {lex_response['sessionState']['intent']['slots']}")

        # Return the response back to the client
        return {
            'statusCode': 200,
            'messages': [message['content'] for message in lex_response['messages']]  # Lex V2 response message
        }

    except Exception as error:
        # Handle errors in the Lambda function or Lex interaction
        print(f"Error sending message to Lex: {error}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'data': {
                    'message': 'Failed to process the request.',
                    'error': str(error)
                }
            })
        }
