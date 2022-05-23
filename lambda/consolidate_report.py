import boto3
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat

BUCKET_NAME = os.environ['BUCKET_NAME']
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '4'))
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '125'))


def list_objects():
    s3 = boto3.client('s3')
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

def download_reports_parallel(bucket, keys, workers=4, chunksize=25):
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]


    with ThreadPoolExecutor(max_workers=workers) as tp:
        for reports in tp.map(download_reports, repeat(bucket), chunks(keys, chunksize)):
            for r in reports:
                yield r

def get_object(s3_client, bucket: str, key: str):
    r = s3_client.get_object(Bucket=bucket, Key=key)
    return r['Body'].read().decode('utf-8')

def download_reports(bucket, keys, **kwargs):
    s3 = boto3.client('s3')
    for k in keys:
        body = get_object(s3, bucket, k)
        yield body

def get_csv_header(report: str):
  return report[:report.find('\n')]

def get_csv_rows(report: str, headerlen: int = None):
  if headerlen:
    return report[headerlen+1:]
  return report[report.find('\n')+1:]

def download_and_consolidate_report(workers: int = 1, batch_size: int = 25):
    
    keys = list(list_objects())
    
    if int(MAX_WORKERS) > 1:
        reports = [e for e in download_reports_parallel(BUCKET_NAME, keys, workers, batch_size)]
    else:
        reports = [e for e in download_reports(BUCKET_NAME, keys)]
    
    if len(reports) == 0:
        return

    header = get_csv_header(reports[0])    

    # Join reports
    body = ''.join(get_csv_rows(r, len(header)) for r in reports)

    consolidated_report = (header + '\n' + body).encode('utf-8')
    
    date = datetime.now().strftime(r"%Y-%m-%d")
    key = f'consolidated/report-{date}.csv'
    
    s3 = boto3.client('s3')
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=consolidated_report)

def handler(event, context):
    download_and_consolidate_report(workers=MAX_WORKERS)

if __name__ == '__main__':
    handler(None, None)