import logging
import os
import json
from urllib.parse import unquote_plus
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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
    df = pd.read_excel(xlsxfile, index_col=0,
                        usecols=['BSI_ID', 'Subject_ID', 'Anatomic_Site', 'Sample_Type','Within_batch_pairing'],
                        converters={'Within_batch_pairing':str})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 's3://pbc-qcarray-sample-info/JIRA/example.parquet')