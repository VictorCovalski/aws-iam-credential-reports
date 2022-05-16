import boto3
import os
from datetime import datetime
from base64 import b64decode


BUCKET_NAME =  os.environ['BUCKET_NAME']

iam = boto3.client('iam', region_name='sa-east-1')

def ensure_plaintext(content):
	try:
		return b64decode(content, validate=True)
	except:
		return content

def generate_credential_report():
	iam.generate_credential_report()

def get_credential_report():
    response = iam.get_credential_report()
    return ensure_plaintext(response['Content'])

def save_credential_report(data, account_id):
	s3 = boto3.client('s3')
	now = datetime.now().strftime("%Y/%m/%d")
	key = f'{now}/report-{account_id}.csv'
	
	s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=data)

def handler(event, context):
    if event['task'] == 'generate_credential_report':
        generate_credential_report()
    elif event['task'] == 'get_credential_report':
        report = get_credential_report()
        account_id = context.invoked_function_arn.split(':')[4]
        save_credential_report(report, account_id)