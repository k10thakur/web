import json
import boto3
import uuid
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

        body = json.loads(event['body'])
        
        name = body.get('name')
        subject = body.get('subject')
        message = body.get('message')
        reveal_time = body.get('revealTime')

        # Basic validation
        if not all([name, subject, message, reveal_time]):
            logger.warning("Validation failed: Missing fields")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({'response_message': 'Missing required fields'})
            }
        
        if len(name) > 32 or len(subject) > 150 or len(message) > 1024:
            logger.warning("Validation failed: Field length exceeded")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({'response_message': 'Field length exceeded'})
            }

        message_id = str(uuid.uuid4())
        message_create_time = int(time.time())

        table.put_item(
            Item={
                'message_id': message_id,
                'name': name,
                'subject': subject,
                'message': message,
                'revealTime': int(reveal_time),
                'messageCreateTime': message_create_time
            }
        )

        logger.info(f"Successfully created message with ID: {message_id}")

        return {
            'statusCode': 201,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'response_message': message_id}, default=default_serializer)
        }
    
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'response_message': 'Something went wrong'})
        }
