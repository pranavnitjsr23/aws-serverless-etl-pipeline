import json
import os
import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):

    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]

    response = glue.start_job_run(
        JobName=os.environ["GLUE_JOB_NAME"],
        Arguments={
            "--bucket_name": bucket_name,
            "--object_key": object_key,
            "--output_bucket": "firm-processed-data"
        }
    )

    return {
        "statusCode": 200,
        "body": response["JobRunId"]
    }
