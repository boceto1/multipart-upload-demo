from flask import Flask, jsonify
from dotenv import load_dotenv
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

@app.route('/')
def hello():
    return AWS_BUCKET

@app.route('/list-multipart-uploads', methods=['GET'])
def list_multipart_uploads():
    try:
        response = s3_client.list_multipart_uploads(Bucket=AWS_BUCKET)
        uploads = response.get('Uploads', [])
        multipart_uploads = []
        for upload in uploads:
            multipart_uploads.append({
                'UploadId': upload['UploadId'],
                'Key': upload['Key'],
                'Initiated': upload['Initiated'].strftime("%Y-%m-%d %H:%M:%S")
            })
        return jsonify(multipart_uploads), 200
    except NoCredentialsError:
        return jsonify({'error': 'Credentials not available'}), 403
    except PartialCredentialsError:
        return jsonify({'error': 'Incomplete credentials provided'}), 403
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/delete-all-multipart-uploads', methods=['DELETE'])
def delete_all_multipart_uploads():
    try:
        # List all multipart uploads
        response = s3_client.list_multipart_uploads(Bucket=AWS_BUCKET)
        uploads = response.get('Uploads', [])

        # Iterate through uploads and abort each one
        for upload in uploads:
            s3_client.abort_multipart_upload(
                Key=upload['Key'],
                UploadId=upload['UploadId'],
                Bucket=AWS_BUCKET,
            )

        return jsonify({'message': 'All multipart uploads have been deleted'}), 200
    except NoCredentialsError:
        return jsonify({'error': 'Credentials not available'}), 403
    except PartialCredentialsError:
        return jsonify({'error': 'Incomplete credentials provided'}), 403
    except Exception as e:
        return jsonify({'error': str(e)}), 500
        
if __name__ == '__main__':
    app.run(debug=True)
