import boto3
import botocore

s3 = boto3.resource('s3')

bucket = s3.Bucket('say-bi-prod')
for item in bucket.objects.all():
    if 'BI_Messages_2016-08-19/' in item.key:
        print(item.get()['Body'].read())