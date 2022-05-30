from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
S3 = S3RemoteProvider()

configfile: "analysis/config.yaml"


rule all:
    input: "test_output.txt"

sample_sheet, = S3.glob_wildcards(config["gtc_bucket"] + "/gtc/" + config["JIRA"] + "/{sample_sheet,SampleSheet\w*.csv}")
rule sample_info:
    input:
        S3.remote(config["manifest"], stay_on_remote=True),
        S3.remote(config["expected_pairings"], stay_on_remote=True),
        S3.remote(expand("{bucket}/gtc/{JIRA}/{sample_sheet}", bucket=config["gtc_bucket"], JIRA=config["JIRA"], sample_sheet=sample_sheet))
    output:
        S3.remote(expand("{bucket}/{JIRA}/sample_info.parquet", bucket=config["sample_info_bucket"], JIRA=config["JIRA"]))
    script:
        "analysis/sample_info.py"

rule batch_mafs:
    params:
        "s3://{bucket}/parquet/{batch}/"
    output:
        S3.remote("{bucket}/maf/{batch}.parquet")
    script:
        "analysis/batch_mafs.R"

batches = set(S3.glob_wildcards(config["gtc_bucket"] + "/parquet/{JIRA}/{part}.parquet")[0])
rule popmaf:
    params:
        expand("s3://{bucket}/maf/", bucket=config["gtc_bucket"])
    input:
        S3.remote(expand("{bucket}/maf/{batch}.parquet", bucket=config["gtc_bucket"], batch=batches)) #, stay_on_remote=True)
    output:
        "popmaf.txt"
    script:
        "analysis/popmaf.R"

rule bafRegress:
    params:
        expand("s3://{bucket}/parquet/{JIRA}/", bucket=config["gtc_bucket"], JIRA=config["JIRA"])
    input:
        "popmaf.txt"
    output:
        "bafRegress.txt"
    script:
        "analysis/bafRegress.R"

rule run_analysis:
    input:
        "bafRegress.txt"
    output:
        "test_output.txt"
    script:
        "analysis/cptac_analysis.R"