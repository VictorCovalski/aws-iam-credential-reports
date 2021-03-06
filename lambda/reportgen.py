import boto3
from botocore.exceptions import ClientError
import csv
import os
from datetime import datetime
from base64 import b64decode
from io import StringIO
from typing import Tuple
from copy import deepcopy


class SecretsManagerSearcher:

    def __init__(self):
        self.__client = boto3.client('secretsmanager')
        self.__cache = self.__load_secrets()

    def __load_secrets(self) -> dict:
        cache = {}
        paginator = self.__client.get_paginator('list_secrets')
        for page in paginator.paginate():
            for secret in page['SecretList']:
                r = self.__client.get_secret_value(SecretId=secret['Name'])
                value = r.get('SecretString', None)
                if value is None:
                    continue
                # Only store secrets that hold Access Key Ids
                if 'AKIA' in value:
                    cache[secret['ARN']] = value
        return cache

    def search(self, secret: str):
        if secret is None:
          return NA
        for k, v in self.__cache.items():
          if secret in v:
            return k
        return NOT_FOUND

NA = 'N/A'
NOT_FOUND = 'NOT FOUND'

BUCKET_NAME =  os.environ['BUCKET_NAME']

iam = boto3.client('iam', region_name='sa-east-1')

def ensure_plaintext(content) -> str:
    try:
        c =  b64decode(content, validate=True)
    except:
        c = content
    finally:
      return c.decode('utf-8')

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

def get_user_access_keys(username: str) -> Tuple[str, str]:
    try:
        r = iam.list_access_keys(UserName=username)
        aks = r['AccessKeyMetadata']
    except ClientError as e:
        return 'Error', 'Error'

    if len(aks) == 2:
        ak1, ak2 = (ak['AccessKeyId'] for ak in aks)
    elif len(aks) == 1:
        ak1, ak2 = aks[0]['AccessKeyId'], NA
    else:
        ak1, ak2 = NA, NA
    return ak1, ak2


def enrich_report(report: str, account_id: str) -> str:
    # Load Secrets from Secrets Manager
    searcher = SecretsManagerSearcher()

    f_in = StringIO(report)
    reader = csv.DictReader(f_in, delimiter=',')

    # Include extra fields for contextual information
    extra_fields = ['access_key_id_1', 'access_key_id_2',
										'access_key_id_1_location', 'access_key_id_2_location',
										'account_id']
		
    fieldnames = deepcopy(reader.fieldnames)
    fieldnames.extend(extra_fields)

    f_out = StringIO()
    writer = csv.DictWriter(f_out, fieldnames=fieldnames, delimiter=',')
    writer.writeheader()

    for user in reader:
        # Include Account ID
        user['account_id'] = account_id

        # Obtain Access Key ID information
        ak1, ak2 = None, None
        if user['access_key_1_active'].lower() == 'true' \
          or user['access_key_2_active'].lower() == 'true':
            ak1, ak2 = get_user_access_keys(user['user'])
            user['access_key_id_1'], user['access_key_id_2'] = ak1, ak2
        else:
            user['access_key_id_1'], user['access_key_id_2'] = NA, NA

        # Search Secrets Manager for stored credentials
        user['access_key_id_1_location'] = searcher.search(ak1)
        user['access_key_id_2_location'] = searcher.search(ak2)

        # Write row to output
        writer.writerow(user)

    f_out.flush()
    f_out.seek(0)
    return f_out.read()

def handler(event, context):
    if event['task'] == 'generate_credential_report':
        generate_credential_report()
    elif event['task'] == 'get_credential_report':
        report = get_credential_report()
        account_id = context.invoked_function_arn.split(':')[4]
        report = enrich_report(report, account_id)
        save_credential_report(report, account_id)

if __name__ == '__main__':
	# f = open('report.csv')
	# report = f.read()
  report = get_credential_report()
  r = enrich_report(report, '12345678912')
  print(r)