#!/usr/bin/env python3

import sys
import boto3

jobid = sys.argv[1]

client = boto3.client('batch')
response = client.describe_jobs(jobs=[jobid])
status = response['jobs'][0]['status']

if status == "SUBMITTED":
    print("running")
elif status == "PENDING":
    print("running")
elif status == "RUNNABLE":
    print("running")
elif status == "STARTING":
    print("running")
elif status == "SUCCEEDED":
    print("success")
else:
    print(status.lower())