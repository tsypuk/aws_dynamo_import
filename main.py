import boto3
import json
import gzip
import argparse
import datetime
import multiprocessing
from functools import partial
from tqdm import tqdm

# DynamoDB target table (should be created already)
wcu_import = 500
rcu_import = 1
wcu_post_import = 1
rcu_post_import = 1


def write_items_from_export_chunk_to_dynamodb(region, bucket, table_name, export_chunk):
    session = boto3.Session(region_name=region)
    dynamodb_client = session.client('dynamodb')
    s3_client = session.client('s3')
    # Get the S3 JSON.gz file path from the JSON object
    # print(f"Processing {json_object['dataFileS3Key']}")

    # Read the JSON.gz file from the S3 bucket
    print(f"Processing {export_chunk['dataFileS3Key']} partition with {export_chunk['itemCount']} items.")
    response = s3_client.get_object(Bucket=bucket, Key=export_chunk['dataFileS3Key'])
    gzipped_data = response['Body'].read()

    # Decompress the gzipped data
    json_data = gzip.decompress(gzipped_data).decode('utf-8')

    # Parse the JSON data
    json_data = json_data.strip().split('\n')
    json_items = []
    for item in json_data:
        json_items.append(json.loads(item))

    pbar = tqdm(total=len(json_items))
    for item in json_items:
        dynamodb_client.put_item(
            TableName=table_name,
            Item=item['Item']
        )
        pbar.update()


def process_json_objects(export_chunks, args):
    session = boto3.Session(region_name=args.region)
    dynamodb_client = session.client('dynamodb')

    # Set higher WCU for faster Import
    # dynamodb_client.update_table(
    #     TableName=args.table,
    #     ProvisionedThroughput={
    #         'ReadCapacityUnits': rcu_import,
    #         'WriteCapacityUnits': wcu_import
    #     }
    # )

    # Create a multiprocessing pool with the number of desired workers
    # TODO extract pool size to args
    pool = multiprocessing.Pool(1)
    func = partial(write_items_from_export_chunk_to_dynamodb, args.region, args.bucket, args.table)
    pool.map(func, export_chunks)

    # Close the pool
    pool.close()
    pool.join()

    # Set the Read/Write Capacity Units post import completed
    # dynamodb_client.update_table(
    #     TableName=args.table,
    #     ProvisionedThroughput={
    #         'ReadCapacityUnits': rcu_post_import,
    #         'WriteCapacityUnits': wcu_post_import
    #     }
    # )


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
    process_json_objects(export_chunks, args)
    print("End Time:", datetime.datetime.now())


def load_from_s3(bucket, manifest_json_file_path, s3_client):
    response = s3_client.get_object(Bucket=bucket, Key=manifest_json_file_path)
    json_data = response['Body'].read().decode('utf-8')
    return json_data.strip().split('\n')


if __name__ == "__main__":
    main()
