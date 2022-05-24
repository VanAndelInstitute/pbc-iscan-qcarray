from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
S3 = S3RemoteProvider(stay_on_remote=True)

configfile: "analysis/config.yaml"

coordinates_filename = "InfiniumQCArray-24v1-0_A3_Physical-and-Genetic-Coordinates.txt"
strand_report_filename = "InfiniumQCArray-24v1-0_A3_StrandReport_FDT.txt"


#rule all:
#    input: "test_output.txt"

rule sample_info:
    input:
        S3.remote(config["manifest"]),
        S3.remote(config["expected_pairings"]),
        S3.glob_wildcards(config["gtc_bucket"] + "/gtc/" + config["JIRA"] + "/SampleSheet{suffix}.csv")
    output:
        S3.remote(expand("{bucket}/{batch_name}/sample_info.parquet", bucket=config["sample_info_bucket"], batch_name=config["JIRA"]))
    script:
        "analysis/sample_info.py"

rule batch_maf:
    input:
        S3.glob_wildcards(config["gtc_bucket"] + "/parquet/{JIRA}/{parts}.parquet")
    output:
        S3.glob_wildcards(config["gtc_bucket"] + "/maf/{JIRA}/part-0.parquet")
    script:
        "analysis/batch_mafs.R"

rule bafRegress:
    input:
        S3.glob_wildcards(config["gtc_bucket"] + "/maf/{JIRA}/part-0.parquet"),
        S3.glob_wildcards(config["gtc_bucket"] + "/parquet/" + config["JIRA"] + "/{parts}.parquet")
    output:
        "bafRegress.txt"
    script:
        "analysis/bafRegress.R"

rule run_analysis:
    input:
        "bafRegress.txt",
        S3.glob_wildcards(config["gtc_bucket"] + "/parquet/" + config["JIRA"] + "/{parts}.parquet"),
        coordinates_file = S3.remote(config["gtc_bucket"] + "/" + coordinates_filename),
        strand_report_file = S3.remote(config["gtc_bucket"] + "/" + strand_report_filename)
    params:
        coordinates_filename = coordinates_filename,
        strand_report_filename = strand_report_filename
    output:
        "test_output.txt"
    script:
        "analysis/cptac_analysis.R"