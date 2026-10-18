import json
import boto3
from io import StringIO
import re
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime


def save_parquet_to_s3(df, bucket, key):
    # Convert DataFrame to Parquet
    table = pa.Table.from_pylist(df)

    # Create a buffer to hold the Parquet data
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)

    # Initialize S3 client and upload the Parquet file
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())
    print(f"✅ Parquet saved to s3://{bucket}/{key}")


def read_log_from_s3(bucket, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    log_data = response['Body'].read().decode('utf-8')
    return log_data


def lambda_handler(event, context):
    # 1: read data from the bucket
    # Get bucket and object key from the S3 event trigger
    record = event['Records'][0]
    bucket_name = record['s3']['bucket']['name']
    input_key = record['s3']['object']['key']

    # Step 2: Read log data
    raw_logs = read_log_from_s3(bucket_name, input_key)

    # Split the log entries using the delimiter
    entries = [entry.strip() for entry in raw_logs.split('---') if entry.strip()]

    # Regex pattern to extract data
    log_pattern = re.compile(
        r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(?P<log_level>[A-Za-z0-9_]+)\] '
        r'(?P<component>[^\s]+) - TicketID=(?P<ticket_id>[^\s]+) SessionID=(?P<session_id>[^\s]+)\s*'
        r'IP=(?P<ip>.*?) \| ResponseTime=(?P<response_time>-?\d+)ms \| CPU=(?P<cpu>[\d.]+)% \| EventType=(?P<event_type>.*?) \| Error=(?P<error>\w+)\s*'
        r'UserAgent="(?P<user_agent>.*?)"\s*'
        r'Message="(?P<message>.*?)"\s*'
        r'Debug="(?P<debug>.*?)"\s*'
        r'TraceID=(?P<trace_id>.*)'
    )

    # Extract structured data
    parsed_entries = []
    for entry in entries:
        match = log_pattern.search(entry)
        if match:
            parsed_entries.append(match.groupdict())

    # Data cleaning

    # i) Drop trace_id column
    for record in parsed_entries:
        record.pop('trace_id', None)

    # ii) Remove Negative response time
    parsed_entries = [
        record
        for record in parsed_entries
        if int(record['response_time']) >= 0
    ]

    # iii) typo-fix in log_level
    fix_log_level = {
        'INF0': 'INFO',
        'DEBG': 'DEBUG',
        'warnING': 'WARNING',
        'EROR': 'ERROR'
    }

    for record in parsed_entries:
        record['log_level'] = fix_log_level.get(
            record['log_level'],
            record['log_level']
        )

    # iv) Remove duplicate rows
    unique_entries = []
    seen = set()

    for record in parsed_entries:
        record_tuple = tuple(sorted(record.items()))

        if record_tuple not in seen:
            seen.add(record_tuple)
            unique_entries.append(record)

    parsed_entries = unique_entries

    # v) Change to appropriate data types (response_time, cpu, timestamp, error)
    for record in parsed_entries:
        record['response_time'] = int(record['response_time'])
        record['cpu'] = float(record['cpu'])
        record['error'] = record['error'].lower() == 'true'

        # timestamp conversion
        record['timestamp'] = datetime.strptime(
            record['timestamp'],
            '%Y-%m-%d %H:%M:%S'
        )

    # Save the data (Upload Parquet to S3)
    output_file_name = input_key.split('/')[2].replace('.log', '.parquet')
    output_key = f'support-logs/processed/{output_file_name}'
    save_parquet_to_s3(parsed_entries, bucket_name, output_key)