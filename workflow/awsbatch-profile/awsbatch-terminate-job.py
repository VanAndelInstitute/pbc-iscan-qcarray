#!/usr/bin/env python3

import sys
import boto3

jobid = sys.argv[1]

client = boto3.client('batch')
response = client.terminate_job(
    jobId=jobid,
    reason='snakemake head job canceled'
)