import sys
sys.path.append('/home/ec2-user/App_tier/ImageClassification_AppTier/model')

import boto3
from model import face_recognition

aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = 'us-east-1'
input_bucket_name="1230041516-in-bucket"
output_bucket_name="1230041516-out-bucket"
res_queue_name="1230041516-resp-queue"
req_queue_name="1230041516-req-queue"

#print(sys.path)
sqs = boto3.client(
    'sqs', 
    region_name=aws_region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

s3 = boto3.client(
    's3', 
    region_name=aws_region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)


def poll_req_queue_and_process():
    queue_url_response = sqs.get_queue_url(QueueName=req_queue_name)
    queue_url = queue_url_response['QueueUrl']
    
    #print(queue_url)
    
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )
        
        messages = response.get('Messages',[])
        
        if messages:
            file_name=messages[0]['Body']
            file_content = get_file_content_from_storage(file_name)
            classification = process_file(file_content)
            write_to_output_storage(classification, file_name)
            receipt_handle = messages[0]['ReceiptHandle']
            write_to_outputQueue(file_name)
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            #print(f"File '{file_name}' processed successfully.")
        # else:
            #print("No messages in the queue.")
            
def process_file(file_content):
    return face_recognition.process_image(file_content)
   
def get_file_content_from_storage(file_name):
    response = s3.get_object(Bucket=input_bucket_name, Key=file_name)
    return response['Body'].read()
4
def write_to_output_storage(classification, file_name):
    s3_key=file_name.split('.')[0]
    s3.put_object(Bucket=output_bucket_name, Key=s3_key, Body=classification)
 
def write_to_outputQueue(file_name):
    #print("in write_to_outputQueue", file_name)
    queue_url_response = sqs.get_queue_url(QueueName=res_queue_name)
    queue_url = queue_url_response['QueueUrl']
    sqs.send_message(QueueUrl=queue_url, MessageBody=file_name)
 
poll_req_queue_and_process()