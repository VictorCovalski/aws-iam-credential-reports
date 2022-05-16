import boto3
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

BUCKET_NAME = os.environ['BUCKET_NAME']
MAX_WORKERS = os.environ.get('MAX_WORKERS', '4')
BATCH_SIZE = os.environ.get('BATCH_SIZE', '125')

HEADER=(
    'user,arn,user_creation_time,password_enabled,'
    'password_last_used,password_last_changed,'
    'password_next_rotation,mfa_active,'
    'access_key_1_active,access_key_1_last_rotated,'
    'access_key_1_last_used_date,access_key_1_last_used_region,'
    'access_key_1_last_used_service,access_key_2_active,'
    'access_key_2_last_rotated,access_key_2_last_used_date,'
    'access_key_2_last_used_region,access_key_2_last_used_service,'
    'cert_1_active,cert_1_last_rotated,cert_2_active,cert_2_last_rotated,'
    'account_id'
    )

s3 = boto3.client('s3')

def list_objects():
    now = datetime.now().strftime(r"%Y/%m/%d")
    prefix = f'{now}/'
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        try:
            keys = page['Contents']
        except:
            break
        
        for c in keys:
            yield c['Key']

def adjust_report(body, key):
    fname = key[key.rfind('/')+1:]
    account_id = fname[fname.find('-')+1:fname.find('.')]

    lines = body.splitlines(keepends=False)
    rows = lines[1:]

    for i in range(len(rows)):
        rows[i] += f',{account_id}'
    
    entries = '\n'.join(rows)

    return entries

def download_reports_parallel(bucket, keys, workers=4, chunksize=25):
    def get_reports(keys):
        entries = []
        for k in keys:
            r = s3.get_object(Bucket=bucket, Key=k)
            body = r['Body'].read().decode('utf-8')
            entries.append(adjust_report(body, k))
        return entries

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]
    
    with ThreadPoolExecutor(max_workers=workers) as tp:
        for reports in tp.map(get_reports, chunks(keys, chunksize)):
            for r in reports:
                yield r

def download_reports(bucket, keys, **kwargs):
    
    for k in keys:
        r = s3.get_object(Bucket=bucket, Key=k)
        body = r['Body'].read().decode('utf-8')
        entries = adjust_report(body, k)
        yield entries

def consolidate_report():
    
    keys = list(list_objects())
    if int(MAX_WORKERS) > 1:
        entries = [e for e in download_reports_parallel(BUCKET_NAME, keys, int(MAX_WORKERS), int(BATCH_SIZE))]
    else:
        entries = [e for e in download_reports(BUCKET_NAME, keys)]
    
    if len(entries) == 0:
        return
    
    entries.insert(0, HEADER)
    report = '\n'.join(entries)
    date = datetime.now().strftime(r"%Y-%m-%d")
    key = f'consolidados/report-{date}.csv'
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=report.encode('utf-8'))

def handler(event, context):
    consolidate_report()

if __name__ == '__main__':
    handler(None, None)