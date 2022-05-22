import logging
import os
import json
from urllib.parse import unquote_plus
import awswrangler as wr
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DEBUG = os.environ.get('DEBUG', False)
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

SAMPLE_INFO_BUCKET = os.environ.get('SAMPLE_INFO_BUCKET')

def lambda_handler(event, _context):
    ''' Given S3 upload event, retrieve the image metadata and publish to SNS topic'''
    logger.debug(json.dumps(event))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    folder = os.path.dirname(key)

    # get actual file names
    paths = wr.s3.list_objects(f's3://{bucket}/{folder}/')
    ss_path = manifest_path = exp_pairs_path = None
    for path in paths:
        logger.debug(path)
        if re.search('SampleSheet', path):
            ss_path = path
        elif re.search('manifest', path, re.IGNORECASE):
            manifest_path = path
        elif re.search('expected', path, re.IGNORECASE):
            exp_pairs_path = path
        else:
            pass
    
    if ss_path is None or manifest_path is None or exp_pairs_path is None:
        # not all files are uploaded yet; a subsequent invoke should have them all
        return

    ss_file = os.path.join('/tmp', os.path.basename(ss_path))
    wr.s3.download(ss_path, ss_file)

    sample_sheet = wr.s3.read_csv(ss_path, skiprows=10,
                    names=['Sample_ID','Barcode','Position'],
                    usecols=['Sample_ID','Barcode','Position'],
                    dtype={'Barcode': str})
    convert_pairing = lambda x: "tumor-normal" if "2" in str(x) else "tumor-normal-nat" if "3" in str(x) else None
    manifest = wr.s3.read_excel(manifest_path,
                        usecols=['BSI_ID', 'Subject_ID', 'Anatomic_Site', 'Sample_Type','Within_batch_pairing'],
                        converters={'Within_batch_pairing':convert_pairing })
    manifest.rename(columns={'BSI_ID': 'Sample_ID'}, inplace=True)
    exp_pairs = wr.s3.read_excel(exp_pairs_path,
                        usecols=['BSI_ID_Current', 'BSI_ID_Previous'])
    exp_pairs.rename(columns={'BSI_ID_Current': 'Sample_ID'}, inplace=True)
    exp_pairs_list = exp_pairs.groupby('Sample_ID').agg(list)
    merged = manifest.merge(sample_sheet, how='left', on='Sample_ID').merge(exp_pairs_list, how='left', on='Sample_ID')
    df = merged[merged.Sample_ID != "EMTPY"]

    batch_name = wr.s3.read_csv(ss_path, skiprows=[0,1], header=None, nrows=1, usecols=[1])[1][0]
    wr.s3.to_parquet(df, f's3://{SAMPLE_INFO_BUCKET}/{batch_name}/sample_info.parquet',
        s3_additional_kwargs={'StorageClass': 'INTELLIGENT_TIERING'})