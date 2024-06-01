import boto3
from botocore.client import Config
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")
FILE_PATH = '/Users/jeankarlo/Desktop/final5.mp4'
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
    response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code
    return response.headers['ETag']

def complete_multipart_upload(bucket_name, file_key, upload_id, parts):
    response = s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=file_key,
        UploadId=upload_id,
        MultipartUpload={'Parts': parts}
    )
    return response

def main():
    start_time = time.time()  # Start timing
    file_key = os.path.basename(FILE_PATH)
    upload_id = create_multipart_upload(AWS_BUCKET, file_key)
    parts = []

    part_number = 1
    with open(FILE_PATH, 'rb') as f:
        while True:
            print(f"Generating part {part_number}")
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            
            presigned_url = generate_presigned_url(AWS_BUCKET, file_key, upload_id, part_number)
            
            etag = upload_part(presigned_url, data)
            parts.append({'PartNumber': part_number, 'ETag': etag})
            part_number += 1
            print(f"Part was generated {part_number}")
    
    complete_response = complete_multipart_upload(AWS_BUCKET, file_key, upload_id, parts)
    print(complete_response)

    
    print(f"Complete to create video: {len(parts)}")
    complete_response = complete_multipart_upload(AWS_BUCKET, file_key, upload_id, parts)
    print(f"Just the end: {len(parts)}")
    print(complete_response)

    end_time = time.time()  # End timing
    duration = end_time - start_time
    print(f"Multipart upload completed in {duration:.2f} seconds")

if __name__ == '__main__':
    main()
