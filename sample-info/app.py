from cmath import exp
import logging
import os
import json
from urllib.parse import unquote_plus
import boto3
import re
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


SAMPLE_INFO_BUCKET = os.environ.get('SAMPLE_INFO_BUCKET')
def lambda_handler(event, _context):
    ''' Given S3 upload event, retrieve the image metadata and publish to SNS topic'''
    logger.debug(json.dumps(event))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    folder = os.path.dirname(key)

    # get actual file names
    response = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
    ss_key = manifest_key = exp_pairs_key = None
    for obj in response['Contents']:
        logger.debug(obj['Key'])
        if re.search('SampleSheet', obj['Key']):
            ss_key = obj['Key']
        elif re.search('manifest', obj['Key'], re.IGNORECASE):
            manifest_key = obj['Key']
        elif re.search('expected', obj['Key'], re.IGNORECASE):
            exp_pairs_key = obj['Key']
        else:
            pass
    
    if ss_key is None or manifest_key is None or exp_pairs_key is None:
        # not all files are uploaded yet; a subsequent invoke should have them all
        return

    ss_file = os.path.join('/tmp', os.path.basename(ss_key))
    manifest_file = os.path.join('/tmp', os.path.basename(manifest_key))
    exp_pairs_file = os.path.join('/tmp', os.path.basename(exp_pairs_key))
    s3.download_file(bucket, ss_key, ss_file)
    s3.download_file(bucket, manifest_key, manifest_file)
    s3.download_file(bucket, exp_pairs_key, exp_pairs_file)

    sample_sheet = pd.read_csv(ss_file, skiprows=10,
                    names=['Sample_ID','Barcode','Position'],
                    usecols=['Sample_ID','Barcode','Position'],
                    dtype={'Barcode': str})
    convert_pairing = lambda x: "tumor-normal" if "2" in str(x) else "tumor-normal-nat" if "3" in str(x) else None
    manifest = pd.read_excel(manifest_file,
                        usecols=['BSI_ID', 'Subject_ID', 'Anatomic_Site', 'Sample_Type','Within_batch_pairing'],
                        converters={'Within_batch_pairing':convert_pairing })
    manifest.rename(columns={'BSI_ID': 'Sample_ID'}, inplace=True)
    exp_pairs = pd.read_excel(exp_pairs_file,
                        usecols=['BSI_ID_Current', 'BSI_ID_Previous'])
    exp_pairs.rename(columns={'BSI_ID_Current': 'Sample_ID'}, inplace=True)
    merged = manifest.merge(sample_sheet, how='left', on='Sample_ID').merge(exp_pairs, how='left', on='Sample_ID')
    subset = merged[merged.Sample_ID != "EMTPY"]

    batch_name = pd.read_csv(ss_file, skiprows=[0,1], header=None, nrows=1, usecols=[1])[1][0]
    table = pa.Table.from_pandas(subset, preserve_index=False)
    pq.write_table(table, f's3://{SAMPLE_INFO_BUCKET}/{batch_name}/sample_info.parquet')