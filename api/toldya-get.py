import json
import boto3
import time
import decimal
import logging

# Set up structured logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ToldYaMessages')

# Helper for Decimal -> int/float
def default_serializer(o):
    if isinstance(o, decimal.Decimal):
        return int(o) if o % 1 == 0 else float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

def lambda_handler(event, context):
    try:
        logger.info(f"Received event: {event}")

        # Extract message_id from query string parameters
        query_params = event.get('queryStringParameters') or {}
        message_id = query_params.get('message_id')

        if not message_id:
            logger.warning("Missing message_id in query parameters")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({'response_message': 'Missing message_id in query string'})
            }

        # Fetch item from DynamoDB
        response = table.get_item(Key={'message_id': message_id})
        item = response.get('Item')

        if not item:
            logger.warning(f"Message ID {message_id} not found")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'name': None,
                    'subject': None,
                    'revealTime': None,
                    'messageCreateTime': None,
                    'message': None,
                    'response_message': 'This message does not exist. Invalid message Id'
                })
            }

        # Convert Decimals
        item = json.loads(json.dumps(item, default=default_serializer))

        current_time = int(time.time())
        reveal_time = item.get('revealTime')

        # Decide whether to reveal message
        if current_time < reveal_time:
            message = None
        else:
            message = item.get('message')

        result = {
            'name': item.get('name'),
            'subject': item.get('subject'),
            'revealTime': item.get('revealTime'),
            'messageCreateTime': item.get('messageCreateTime'),
            'message': message
        }

        logger.info(f"Returning result for message ID {message_id}")

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps(result, default=default_serializer)
        }

    except Exception as e:
        logger.error(f"Exception occurred: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'name': None,
                'subject': None,
                'revealTime': None,
                'messageCreateTime': None,
                'message': None,
                'response_message': 'Something went wrong, please try again'
            })
        }
