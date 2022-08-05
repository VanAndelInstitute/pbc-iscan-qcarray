#!/usr/bin/env python3

import sys
import json
import boto3
import argparse
from snakemake.utils import read_job_properties

jobscript = sys.argv[1]

# get the job properties
job_properties = read_job_properties(jobscript)
rule = job_properties["rule"]
jobid = job_properties["jobid"]
threads = job_properties["threads"]
params = job_properties["params"]
memory = job_properties["resources"]["mem_mb"]
job_queue = job_properties["resources"]["queue"]
job_definition = job_properties["resources"]["job_definition"]
timeout = job_properties["resources"]["runtime"]


# get the command to run
def get_jobscript():
    with open(jobscript) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                return line

dependencies = []
resource_requirements = [
    {
        'value': str(threads),
        'type': 'VCPU'
    },
    {
        'value': str(memory),
        'type': 'MEMORY'
    }
]

client = boto3.client('batch')
response = client.submit_job(
    jobName=f'snakejob-{rule}-{jobid}',
    jobQueue=job_queue,
    dependsOn=[{'jobId':dep} for dep in dependencies],
    jobDefinition=job_definition,
    parameters=params,
    containerOverrides={
        'command': ["/bin/bash","-c",get_jobscript()],
        'resourceRequirements': resource_requirements
    },
    timeout={
        'attemptDurationSeconds': timeout
    }
)
print(response["jobId"])
