import os
import requests
import boto3
from botocore.client import Config

AWS_BUCKET = os.getenv("AWS_BUCKET")

def upload_part_with_presigned_url(presigned_url, file_path):
    headers = {'Content-Type': 'video/mp4'}
    with open(file_path, 'rb') as data:
        response = requests.put(presigned_url, headers=headers, data=data)
    return response.status_code, response.text


s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION'),
    config=Config(signature_version='s3v4')
)

bucket_name = AWS_BUCKET
file_key = 'test-video.mp4'
upload_id = 'ybY1ssxqTbTNvBEIoCtQtu4irwaFQ4SH6csxqv8.ZtpOIpq_OLZZ2w8nN6ZrTYekkL7LIQJ0sUaI38b0pZQQlE7vqc15D3CId0Xf2rBACenTAM3j04rOvKV1vK4QdXnZ'

response = s3_client.list_parts(
    Bucket=bucket_name,
    Key=file_key,
    UploadId=upload_id
)

print(response)
