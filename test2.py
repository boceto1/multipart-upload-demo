import boto3
from botocore.client import Config
import os
import requests

# Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")
FILE_PATH = '/Users/jeankarlo/Desktop/final4.mp4'
CHUNK_SIZE = 5 * 1024 * 1024  # 5MB

# Initialize S3 client
s3_client = boto3.client('s3', 
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                         region_name=AWS_REGION,
                         config=Config(signature_version='s3v4'))

def create_multipart_upload(bucket_name, file_key):
    response = s3_client.create_multipart_upload(Bucket=bucket_name, Key=file_key)
    return response['UploadId']

def generate_presigned_url(bucket_name, file_key, upload_id, part_number, expiration=3600):
    presigned_url = s3_client.generate_presigned_url(
        ClientMethod='upload_part',
        Params={
            'Bucket': bucket_name,
            'Key': file_key,
            'PartNumber': part_number,
            'UploadId': upload_id
        },
        ExpiresIn=expiration
    )
    return presigned_url

def upload_part(presigned_url, data):
    response = requests.put(presigned_url, data=data)
    return response

def complete_multipart_upload(bucket_name, file_key, upload_id, parts):
    response = s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=file_key,
        UploadId=upload_id,
        MultipartUpload={'Parts': parts}
    )
    return response

def main():
    print("Start to upload video")
    file_key = os.path.basename(FILE_PATH)
    upload_id = create_multipart_upload(AWS_BUCKET, file_key)
    print(f"I created multipart upload: {upload_id}")
    parts = []

    part_number = 1
    with open(FILE_PATH, 'rb') as f:
        while True:
            print(f"Creating part: {part_number}")
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            
            presigned_url = generate_presigned_url(AWS_BUCKET, file_key, upload_id, part_number)
            print(f"presigned_url: {presigned_url} ,{part_number}")
            response = upload_part(presigned_url, data)
            print(f"Upload chunck: {presigned_url} ,{part_number}")
            
            if response.status_code != 200:
                print(f"Failed to upload part {part_number}")
                return
            
            etag = response.headers['ETag']
            parts.append({'PartNumber': part_number, 'ETag': etag})
            print(f"Finish to upload: {presigned_url} ,{part_number}")
            part_number += 1
    
    print(f"Complete to create video: {len(parts)}")
    complete_response = complete_multipart_upload(AWS_BUCKET, file_key, upload_id, parts)
    print(f"Just the end: {len(parts)}")
    print(complete_response)

if __name__ == '__main__':
    main()
