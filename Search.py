# search-s3 files
import boto3
if __name__ == '__main__':
    s3 = boto3.client('s3')
    
    bucket='project2-targetlanding-parquet'
    prefix='onpremise/restaurant/'

    """
    # Read matching prefix data within an file in S3 bucket
    s3 = boto3.resource('s3')
    for obj in s3.Bucket(bucket).objects.filter(Prefix= prefix):
        print(obj.key)
    
    # Read matching prefix data within an file in S3 bucket
    result = s3.list_objects_v2(Bucket = bucket, Prefix = prefix)
    for obj in result.get('Contents'):
        print(obj.get('Key'))
    
    """
    
    match_files = [] 
    result = s3.list_objects_v2(Bucket = bucket, Prefix = prefix)
    for obj in result.get('Contents'):
        key = obj.get('Key')
        filename = key.split('/')[-1]
        date = filename[0:8]
        if date in ['20220928','20220927']:
            match_files.append(key)

    match_files
    





