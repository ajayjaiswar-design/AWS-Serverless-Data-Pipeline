import json
import boto3

glue = boto3.client('glue')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    input_key = event['Records'][0]['s3']['object']['key']

    s3_input_path = f"s3://{bucket}/{input_key}"

    print(f"Triggering Glue job with file: {s3_input_path}")

    glue.start_job_run(
        JobName='Automate_ETL_support_tickets',
        Arguments={
            '--input_file_path': s3_input_path
        }
    )