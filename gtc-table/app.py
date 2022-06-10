import logging
import os
import json
from urllib.parse import unquote_plus
import awswrangler as wr
import pandas as pd
from IlluminaBeadArrayFiles import GenotypeCalls, BeadPoolManifest, code2genotype

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DEBUG = os.environ.get('DEBUG', False)
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

MANIFEST_FILENAME = os.environ.get('MANIFEST_FILENAME')

def lambda_handler(event, _context):
    """ Extract the binary .gtc file data into a Parquet dataset for two reasons:
        1. The gencall algorithm doesn't automatically calculate normalized intensity values, which can take
           a while when processing multiple samples at the same time.
        2. It makes the downstream analysis code slightly simpler.
    """
    logger.debug(json.dumps(event))
    bucket = event['Records'][0]['s3']['bucket']['name']
    gtc_key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    gtc_prefix = os.path.dirname(gtc_key)
    gtc_filename = os.path.basename(gtc_key)
    gtc_basename, _ = os.path.splitext(gtc_filename)
    barcode, position = gtc_basename.split('_')
    logger.debug(barcode + '_' + position)

    # Fetch the sample ID from the metadata
    sample_sheet = wr.s3.read_csv(f's3://{bucket}/{gtc_prefix}/SampleSheet*.csv', skiprows=10,
                    names=['Sample_ID','Barcode','Position'],
                    usecols=['Sample_ID','Barcode','Position'],
                    dtype={'Barcode': str})
    sample_ids = sample_sheet.loc[(sample_sheet['Barcode'] == barcode) & (sample_sheet['Position'] == position),
        "Sample_ID"].values
    if len(sample_ids) == 0:
        # This .idat doesn't belong to this batch, so skip it
        return
    sample_id = sample_ids[0]

    # Extract the gencall data from the .gtc and create a pyarrow table
    wr.s3.download(path=f's3://{bucket}/{MANIFEST_FILENAME}', local_file='/tmp/' + MANIFEST_FILENAME)
    manifest = BeadPoolManifest('/tmp/' + MANIFEST_FILENAME)
    wr.s3.download(path=f's3://{bucket}/{gtc_key}', local_file='/tmp/' + gtc_filename)
    gtc = GenotypeCalls('/tmp/' + gtc_filename)
    forward_calls = gtc.get_base_calls_forward_strand(manifest.snps, manifest.source_strands)
    call1,call2 = map(list, zip(*[(call[0],call[-1]) for call in forward_calls]))
    normalized_intensities = gtc.get_normalized_intensities(manifest.normalization_lookups)
    x,y = map(list, zip(*normalized_intensities))
    df = pd.DataFrame({
        'Sample_ID': sample_id,
        'Gender': gtc.get_gender(),
        'marker': manifest.names,
        'x': x,
        'y': y,
        'call1': call1,
        'call2': call2,
        'baf': gtc.get_ballele_freqs(),
        'abgeno': [code2genotype[code] for code in gtc.get_genotypes()]
    })

    # Store parquet dataset with same folder structure as .gtc files
    pq_filename = gtc_filename.replace('.gtc', '.parquet')
    wr.s3.to_parquet(df, path=f's3://{bucket}/{gtc_prefix.replace("gtc","parquet")}/{pq_filename}',
        s3_additional_kwargs={'StorageClass': 'INTELLIGENT_TIERING'})

if __name__ == "__main__":
    """
    Convenience code for generating .parquet files from all .gtc files in a batch.
    Export env var MANIFEST_FILENAME="InfiniumQCArray-24v1-0_A4.bpm" first.
    """
    import argparse
    import boto3

    parser = argparse.ArgumentParser(description='Create Parquet files for all .gtc files in a folder.')
    parser.add_argument('bucket', metavar='bucket',
                help='the bucket containing the .gtc files')
    parser.add_argument('batch_name', metavar='batch_name',
                help='the folder containing the .gtc files')
    args = parser.parse_args()

    s3 = boto3.client('s3')
    resp = s3.list_objects_v2(Bucket=args.bucket, Prefix=f'gtc/{args.batch_name}')
    keys = [obj['Key'] for obj in resp['Contents'] if obj['Key'].endswith('.gtc')]
    for key in keys:
        event = {
                    "Records": [
                        {
                            "s3": {
                                "bucket": {
                                    "name": args.bucket
                                },
                                "object": {
                                    "key": key
                                }
                            }
                        }
                    ]
                }
        lambda_handler(event, None)