import logging
import os
import json
from urllib.parse import unquote_plus
import boto3
from xlsx2csv import Xlsx2csv
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DEBUG = os.environ.get('DEBUG', False)
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, _context):
    ''' Given S3 upload event, retrieve the image metadata and publish to SNS topic'''
    logger.debug(json.dumps(event))
    bucket = event['Records'][0]['s3']['bucket']['name']
    xlsxkey = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    xlsxfile = f'/tmp/{os.path.basename(xlsxkey)}'

    # download xlsx
    s3.download_file(bucket, xlsxkey, xlsxfile)
    csvfile = f'{os.path.splitext(xlsxfile)[0]}.csv'

    # convert
    Xlsx2csv(xlsxfile, outputencoding="utf-8").convert(csvfile)
    
    # remove "empty" lines
    with open(csvfile, "r") as f:
        lines = f.readlines()
    with open(csvfile, "w") as f:
        for line in lines:
            if re.search(r',,,,,,,,,,,', line) is None:
                f.write(line)
    
    # upload csv
    csvkey = f'{os.path.splitext(xlsxkey)[0]}.csv'
    s3.upload_file(csvfile, bucket, csvkey)
