import boto3
from botocore.client import Config
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    parts = sorted(parts, key=lambda x: x['PartNumber']) #Super important
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
    futures = []

    with ThreadPoolExecutor(max_workers=5) as executor:
      with open(FILE_PATH, 'rb') as f:
          while True:
              print(f"Creating part: {part_number}")
              data = f.read(CHUNK_SIZE)
              if not data:
                  break
                
              presigned_url = generate_presigned_url(AWS_BUCKET, file_key, upload_id, part_number)
              futures.append(executor.submit(upload_part, presigned_url, data))
              part_number += 1

      for future in as_completed(futures):
        try:
            print(f"Part was created: {futures.index(future) + 1}")
            etag = future.result()
            parts.append({'PartNumber': futures.index(future) + 1, 'ETag': etag})
        except Exception as exc:
                print(f'Part upload generated an exception: {exc}')

    
    print(f"Complete to create video: {len(parts)}")
    complete_response = complete_multipart_upload(AWS_BUCKET, file_key, upload_id, parts)
    print(f"Just the end: {len(parts)}")
    print(complete_response)

if __name__ == '__main__':
    main()
