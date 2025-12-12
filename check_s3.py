import boto3
from dotenv import load_dotenv
import os
import json

load_dotenv('.env')

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

bucket = os.getenv('AWS_S3_BUCKET')

# List Silver layer objects
print(f"Bucket: {bucket}")
print(f"\n=== Silver Layer (processed/jobs/) ===")
response = s3.list_objects_v2(Bucket=bucket, Prefix='processed/jobs/', MaxKeys=10)
print(f"Total objects: {response.get('KeyCount', 0)}")

for obj in response.get('Contents', [])[:3]:
    print(f"\n--- {obj['Key']} ---")
    
    # Get and print file content
    file_response = s3.get_object(Bucket=bucket, Key=obj['Key'])
    data = json.loads(file_response['Body'].read().decode('utf-8'))
    
    print(f"Title: {data.get('title', 'N/A')}")
    print(f"Company: {data.get('company', 'N/A')}")
    print(f"Salary: {data.get('salary', 'N/A')}")
    print(f"Location: {data.get('location', 'N/A')}")
    print(f"Skills: {data.get('skills', [])}")
