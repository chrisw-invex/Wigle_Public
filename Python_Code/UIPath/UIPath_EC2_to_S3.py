import boto3
s3 = boto3.resource('s3')
s3.Object('***', '***/***').put(Body=open('***', 'rb'))
