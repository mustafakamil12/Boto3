import boto3
import json


QUEUE_NAME = 'MyTest-SQS-Queue'
QUEUE_NAME_URL = 'https://queue.amazonaws.com/030871675061/MyTest-SQS-Queue'
FIFO_QUEUE_NAME = 'MyTestQueue.fifo'        #mandatory to be understandable by amazon.
FIFO_QUEUE_NAME_URL = 'https://queue.amazonaws.com/030871675061/MyTestQueue.fifo'
DEAD_LETTER_QUEUE_NAME = 'Dead-Letter-Queue-for-Main'
MAIN_QUEUE = 'Main-Queue'
MAIN_QUEUE_URL = 'https://queue.amazonaws.com/030871675061/Main-Queue'

def sqs_client():
    sqs = boto3.client('sqs', region_name='us-east-1')
    """:type : pyboto3.sqs """
    return sqs

def create_sqs_queue():
    return sqs_client().create_queue(
        QueueName=QUEUE_NAME
    )

def create_fifo_queue():
    return sqs_client().create_queue(
        QueueName=FIFO_QUEUE_NAME,
        Attributes={
            'FifoQueue': 'true'
        }
    )

def create_queue_for_dead_letter():
    return sqs_client().create_queue(
        QueueName=DEAD_LETTER_QUEUE_NAME
    )

def create_main_queue_for_dead_letter():
    redrive_policy = {
        "deadLetterTargetArn": "arn:aws:sqs:us-east-1:030871675061:Dead-Letter-Queue-for-Main",
        "maxReceiveCount": 3
    }
    return sqs_client().create_queue(
        QueueName=MAIN_QUEUE,
        Attributes={
            "DelaySeconds": "0",
            "MaximumMessageSize": "262144",
            "VisibilityTimeout": "300",
            "MessageRetentionPeriod": "345600",
            "ReceiveMessageWaitTimeSeconds": "0",
            "RedrivePolicy": json.dumps(redrive_policy)
        }
    )

def find_queue():
    return sqs_client().list_queues(
        QueueNamePrefix='MyTest'
    )

def list_all_queues():
    return sqs_client().list_queues()

def get_queue_attributes():
    return sqs_client().get_queue_attributes(
        QueueUrl=MAIN_QUEUE_URL,
        AttributeNames=['All']
    )

def update_queue_attributes():
    return sqs_client().set_queue_attributes(
        QueueUrl=MAIN_QUEUE_URL,
        Attributes={
            "MaximumMessageSize": "131072",
            "VisibilityTimeout": "600"
        }
    )

def delete_queue():
    return sqs_client().delete_queue(
        QueueUrl=QUEUE_NAME_URL
    )

def send_message_to_queue():
    return sqs_client().send_message(
        QueueUrl=MAIN_QUEUE_URL,
        MessageAttributes={
            'Title':{
                'DataType': 'String',
                'StringValue': 'My Message Title'
            },
            'Author': {
                'DataType': 'String',
                'StringValue': 'Mustafa'
            },
            'Time': {
                'DataType': 'String',
                'StringValue': '6'
            }
        },
        MessageBody='This is my first SQS Message!'
    )

def send_batch_messages_to_queue():
    return sqs_client().send_message_batch(
        QueueUrl=MAIN_QUEUE_URL,
        Entries=[
            {
                'Id': 'FirstMessage',
                'MessageBody': 'This is the 1st message of batch'
            },
            {
                'Id': 'SecondMessage',
                'MessageBody': 'This is the 2nd message of batch'
            },
            {
                'Id': 'ThirdMessage',
                'MessageBody': 'This is the 3rd message of batch'
            },
            {
                'Id': 'FourthMessage',
                'MessageBody': 'This is the 4th message of batch'
            }
        ]
    )

def poll_queue_for_messages():
    return sqs_client().receive_message(
        QueueUrl=MAIN_QUEUE_URL,
        MaxNumberOfMessages=10
    )

def process_message_from_queue():
    queued_messages = poll_queue_for_messages()
    if 'Messages' in queued_messages and len(queued_messages['Messages']) >= 1:
        for message in queued_messages['Messages']:
            print("Processing message " + message['MessageId'] + "with text " + message['Body'])
            #delete_message_from_queue(message['ReceiptHandle'])
            change_message_visibility_timeout(message['ReceiptHandle'])

def delete_message_from_queue(receipt_handle):
    sqs_client().delete_message(
        QueueUrl=MAIN_QUEUE_URL,
        ReceiptHandle=receipt_handle
    )

def change_message_visibility_timeout(receipt_handle):
    sqs_client().change_message_visibility(
        QueueUrl=MAIN_QUEUE_URL,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=360
    )

def purge_queue():
    return sqs_client().purge_queue(
        QueueUrl=MAIN_QUEUE_URL
    )

if __name__ == '__main__':
    # print(create_sqs_queue())
    #print(create_fifo_queue())
    #create_queue_for_dead_letter()
    #print(create_main_queue_for_dead_letter())
    #print(find_queue())
    #print(list_all_queues())
    #print(get_queue_attributes())
    #print(update_queue_attributes())
    #delete_queue()
    #send_message_to_queue()
    #send_batch_messages_to_queue()
    #print(poll_queue_for_messages())
    #process_message_from_queue()
    purge_queue()
