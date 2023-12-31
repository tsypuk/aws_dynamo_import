import time

import boto3
import json
import gzip
import argparse
import datetime
import multiprocessing
from tqdm import tqdm

# DynamoDB target table (should be created already)
wcu_import_single_process = 35
rcu_import = 1
wcu_post_import = 1
rcu_post_import = 1


def write_items_to_dynamodb(json_data, args):
    pbar = tqdm(total=len(json_data))
    session = boto3.Session(region_name=args.region)
    dynamodb_client = session.client('dynamodb')
    for item in json_data:
        dynamodb_client.put_item(
            TableName=args.table,
            Item=json.loads(item)['Item']
        )
        pbar.update()


def process_export_chunks(export_chunks, data_count, args):
    session = boto3.Session(region_name=args.region)
    s3_client = session.client('s3')
    dynamodb_client = session.client('dynamodb')

    # Set higher WCU for faster Import
    dynamodb_client.update_table(
        TableName=args.table,
        ProvisionedThroughput={
            'ReadCapacityUnits': rcu_import,
            'WriteCapacityUnits': wcu_import_single_process * int(args.pool)
        }
    )

    chunk_size = data_count // int(args.pool)
    print(f"Chunk size is per process: {chunk_size}")
    result = []
    with multiprocessing.Pool(processes=int(args.pool)) as pool:
        for export_chunk in export_chunks:
            print(f"Processing {export_chunk['itemCount']} items from {export_chunk['dataFileS3Key']}")
            response = s3_client.get_object(Bucket=args.bucket, Key=export_chunk['dataFileS3Key'])
            gzipped_data = response['Body'].read()
            json_data = gzip.decompress(gzipped_data).decode('utf-8')
            json_data = json_data.strip().split('\n')
            for item in json_data:
                result.append(item)
                if len(result) >= chunk_size:
                    pool.apply_async(write_items_to_dynamodb, (result, args))
                    result = []
        if len(result) > 0:
            pool.apply_async(write_items_to_dynamodb, (result, args))

        # Wait for all tasks to complete
        pool.close()
        pool.join()

        time.sleep(5)

        print("Wait 5 seconds to free WCU")
        # Set the Read/Write Capacity Units post import completed
        dynamodb_client.update_table(
            TableName=args.table,
            ProvisionedThroughput={
                'ReadCapacityUnits': rcu_post_import,
                'WriteCapacityUnits': wcu_post_import
            }
        )


def show_stat(json_summary):
    print(f"""
    S3 bucket with export: {json_summary['s3Bucket']}
    S3 SSE algorithm: {json_summary['s3SseAlgorithm']}
    export output format: {json_summary['outputFormat']}
    
    Export ARN: {json_summary['exportArn']}
    Export duration: {json_summary['startTime']} - {json_summary['endTime']}
    Export execution time: {json_summary['exportTime']}
    
    Source Exported Table: {json_summary['tableArn']}
    Items count: {json_summary['itemCount']}
    """)


def main():
    print("Start Time:", datetime.datetime.now())

    parser = argparse.ArgumentParser(description='Write an item to DynamoDB')

    parser.add_argument('--table', type=str, help='DynamoDB target table name', required=True)
    parser.add_argument('--bucket', type=str, help='S3 bucket with DynamoDB backup', required=True)
    parser.add_argument('--export', type=str, help='DynamoDB Export id in S3', required=True)
    parser.add_argument('--region', type=str, help='AWS region', required=False, default='eu-west-1')
    parser.add_argument('--pool', type=str, help='Multiprocessing #', required=False, default='1')
    args = parser.parse_args()

    session = boto3.Session(region_name=args.region)
    s3_client = session.client('s3')

    manifest_summary_json_file_path = f"AWSDynamoDB/{args.export}/manifest-summary.json"

    json_summary = load_from_s3(args.bucket, manifest_summary_json_file_path, s3_client)
    data = json.loads(json_summary[0])
    show_stat(data)

    manifest_json_file_path = data['manifestFilesS3Key']

    json_manifest = load_from_s3(args.bucket, manifest_json_file_path, s3_client)

    export_chunks = []
    data_count = 0

    # Process each JSON object
    for json_obj in json_manifest:
        data = json.loads(json_obj)
        # print(data)
        if data['itemCount'] > 0:
            export_chunks.append(data)
            data_count = data_count + data['itemCount']

    print(f'Items count calculated in export chunks: {data_count}')
    process_export_chunks(export_chunks, data_count, args)
    print("End Time:", datetime.datetime.now())


def load_from_s3(bucket, manifest_json_file_path, s3_client):
    response = s3_client.get_object(Bucket=bucket, Key=manifest_json_file_path)
    json_data = response['Body'].read().decode('utf-8')
    return json_data.strip().split('\n')


if __name__ == "__main__":
    main()
