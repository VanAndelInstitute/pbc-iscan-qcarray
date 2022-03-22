from glob import glob
import logging
import os
import json
from random import sample
from urllib.parse import unquote_plus
import boto3
from datetime import datetime, timedelta
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DEBUG = os.environ.get('DEBUG', False)
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

def lambda_handler(event, _context):
    ''' Given S3 upload event, retrieve the image metadata and publish to SNS topic'''
    logger.debug(json.dumps(event))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    sample = key[:-9]
    logger.info('idat prefix = %s', sample)
    event_time = datetime.fromisoformat(event['Records'][0]['eventTime'][:-1])

    s3 = boto3.client('s3')
    s3.download_file(bucket, f'{sample}_Red.idat', f'/tmp/{sample}_Red.idat')
    # wait up to 5 seconds for Grn file, but make sure it's not an old version
    waiter = s3.get_waiter('object_exists')
    waiter.wait(Bucket=bucket, Key=f'{sample}_Grn.idat', IfModifiedSince=event_time-timedelta(minutes=1), WaiterConfig={'MaxAttempts': 1})
    s3.download_file(bucket, f'{sample}_Grn.idat', f'/tmp/{sample}_Grn.idat')
    
    # download manifest and cluster file, if not already in /tmp
    bpm = 'InfiniumQCArray-24v1-0_A4.bpm'
    egt = 'InfiniumQCArray-24v1-0_A3_ClusterFile.egt'
    if not os.path.exists(f'/tmp/{bpm}'):
        s3.download_file(bucket, bpm, f'/tmp/{bpm}')
    if not os.path.exists(f'/tmp/{egt}'):
        s3.download_file(bucket, egt, f'/tmp/{egt}')
    
    subprocess.run(['/opt/iaap-cli/iaap-cli', 'gencall', f'/tmp/{bpm}', f'/tmp/{egt}', '/tmp', '-f', '/tmp', '-g'])
    subprocess.run(['/opt/iaap-cli/iaap-cli', 'gencall', f'/tmp/{bpm}', f'/tmp/{egt}', '/tmp', '-f', '/tmp', '-p'])
    
    s3.upload_file(f'/tmp/{sample}.gtc', bucket, f'{sample}.gtc')
    s3.upload_file(f'/tmp/{sample}.ped', bucket, f'{sample}.ped')

    # delete the files for this sample so that we don't run gencall against them in subsequent runs and don't fill up /tmp
    for filename in glob(f'/tmp/{sample}*'):
        os.remove(filename)

    