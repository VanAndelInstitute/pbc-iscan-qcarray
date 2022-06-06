import sys
import awswrangler as wr
import pyarrow as pa
import pyarrow.parquet as pq

gtc_bucket = snakemake.config["gtc_bucket"]
batch_name = snakemake.config["JIRA"]
manifest = snakemake.config["manifest"]
expected_pairings = snakemake.config["expected_pairings"]
output_file = snakemake.output[0]

# SampleSheet*.csv
sample_sheet = wr.s3.read_csv(f's3://{gtc_bucket}/gtc/{batch_name}/SampleSheet*.csv', skiprows=10,
                names=['Sample_ID','Barcode','Position'],
                usecols=['Sample_ID','Barcode','Position'],
                dtype={'Barcode': str})

# Excel manifest file
convert_pairing = lambda x: "tumor-normal" if "2" in str(x) else "tumor-normal-nat" if "3" in str(x) else None
manifest = wr.s3.read_excel(f's3://{manifest}',
                    usecols=['BSI_ID', 'Subject_ID', 'Anatomic_Site', 'Sample_Type','Within_batch_pairing'],
                    converters={'Within_batch_pairing':convert_pairing })
manifest.rename(columns={'BSI_ID': 'Sample_ID'}, inplace=True)

# Excel expected pairs file
exp_pairs = wr.s3.read_excel(f's3://{expected_pairings}',
                    usecols=['BSI_ID_Current', 'BSI_ID_Previous'])
exp_pairs.rename(columns={'BSI_ID_Current': 'Sample_ID'}, inplace=True)
exp_pairs_list = exp_pairs.groupby('Sample_ID').agg(list)

# Combine files
merged = manifest.merge(sample_sheet, how='left', on='Sample_ID').merge(exp_pairs_list, how='left', on='Sample_ID')
df = merged[merged.Sample_ID != "EMTPY"]

# Write to Parquet column store
table = pa.Table.from_pandas(df)
pq.write_table(table, output_file)