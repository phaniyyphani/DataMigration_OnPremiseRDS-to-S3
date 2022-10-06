# read_s3.py
import boto3
import json
if __name__ == '__main__':
    s3 = boto3.client('s3')
    
    # List Buckets
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')





        

    # Read whole data within an file in S3 bucket
    bucket='project2-inputfiles'
    prefix='FullLoad.config'
    result = s3.list_objects_v2(Bucket = bucket, Prefix = prefix)
    for o in result.get('Contents'):
        data=s3.get_object(Bucket=bucket, Key=o.get('Key'))
        contents=data['Body'].read()
        contentsInS3File=contents.decode("utf-8")
        print(contentsInS3File)
    
    # Read data within a file in S3 bucket as JSON and read content within the JSON
    bucket='project2-inputfiles'
    key='FullLoad.config'
    response = s3.get_object(Bucket = bucket, Key = key)
    content = response['Body']
    jsonObject = json.loads(content.read())
    print(jsonObject)
    print(jsonObject["source"]["database"])
