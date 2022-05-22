from glob import glob
import logging
import os
import json
from urllib.parse import unquote_plus
import boto3
import botocore
from datetime import datetime, timedelta
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DEBUG = os.environ.get('DEBUG', False)
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
waiter = s3.get_waiter('object_exists')
MANIFEST_CLUSTER_BUCKET = os.environ.get('MANIFEST_CLUSTER_BUCKET', False)
BPM = 'InfiniumQCArray-24v1-0_A4.bpm'
EGT = 'InfiniumQCArray-24v1-0_A3_ClusterFile.egt'
# download manifest and cluster file if not already in /tmp
if not os.path.exists(f'/tmp/{BPM}'):
    s3.download_file(MANIFEST_CLUSTER_BUCKET, BPM, f'/tmp/{BPM}')
if not os.path.exists(f'/tmp/{EGT}'):
    s3.download_file(MANIFEST_CLUSTER_BUCKET, EGT, f'/tmp/{EGT}')

def lambda_handler(event, _context):
    ''' Given S3 upload event, retrieve the image metadata and publish to SNS topic'''
    logger.debug(json.dumps(event))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    sample = key[:-9]
    logger.info('idat prefix = %s', sample)
    event_time = datetime.fromisoformat(event['Records'][0]['eventTime'][:-1])

    # skip processing the new idat if we also have a (fresh) gtc
    try:
        s3.head_object(Bucket=bucket, Key=f'{sample}.gtc', IfModifiedSince=event_time-timedelta(minutes=1))
        logger.info('%s.gtc already exists. Skipping.', sample)
        return
    except botocore.exceptions.ClientError:
        logger.info('%s.gtc missing or not current. Generating...', sample)
        pass

    s3.download_file(bucket, f'{sample}_Red.idat', f'/tmp/{sample}_Red.idat')
    # wait up to 5 seconds for Grn file, but make sure it's not an old version
    waiter.wait(Bucket=bucket, Key=f'{sample}_Grn.idat', IfModifiedSince=event_time-timedelta(minutes=1), WaiterConfig={'MaxAttempts': 1})
    s3.download_file(bucket, f'{sample}_Grn.idat', f'/tmp/{sample}_Grn.idat')
    
    cmd = f'/opt/iaap-cli/iaap-cli gencall /tmp/{BPM} /tmp/{EGT} /tmp -f /tmp -g'
    subprocess.run(cmd)
    
    s3.upload_file(f'/tmp/{sample}.gtc', bucket, f'{sample}.gtc',
        ExtraArgs={'StorageClass': 'INTELLIGENT_TIERING'})

    # delete the files for this sample so that we don't run gencall against them in subsequent runs and don't fill up /tmp
    for filename in glob(f'/tmp/{sample}*'):
        os.remove(filename)

    