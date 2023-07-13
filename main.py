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


def write_item_to_dynamodb(region, bucket, table_name, json_object):
    session = boto3.Session(region_name=region)
    dynamodb_client = session.client('dynamodb')
    s3_client = session.client('s3')
    # Get the S3 JSON.gz file path from the JSON object
    # print(f"Processing {json_object['dataFileS3Key']}")

    # Read the JSON.gz file from the S3 bucket
    print(f"Processing {json_object['dataFileS3Key']} partition with {json_object['itemCount']} items.")
    response = s3_client.get_object(Bucket=bucket, Key=json_object['dataFileS3Key'])
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


def process_json_objects(json_objects, args):
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
    pool = multiprocessing.Pool(2)
    func = partial(write_item_to_dynamodb, args.region, args.bucket, args.table)
    pool.map(func, json_objects)

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


def main():
    print("Start Time:", datetime.datetime.now())

    parser = argparse.ArgumentParser(description='Write an item to DynamoDB')

    parser.add_argument('--table', type=str, help='DynamoDB target table name', required=True)
    parser.add_argument('--bucket', type=str, help='S3 bucket with DynamoDB backup', required=True)
    parser.add_argument('--export', type=str, help='DynamoDB Export id in S3', required=True)
    parser.add_argument('--region', type=str, help='AWS region', required=False, default='eu-west-1')
    args = parser.parse_args()

    manifest_json_file_path = f"AWSDynamoDB/{args.export}/manifest-files.json"

    # TODO Add information about the import

    # Read the JSON file from the S3 bucket
    session = boto3.Session(region_name=args.region)
    s3_client = session.client('s3')
    response = s3_client.get_object(Bucket=args.bucket, Key=manifest_json_file_path)
    json_data = response['Body'].read().decode('utf-8')
    json_objects = json_data.strip().split('\n')

    manifest_chunks = []
    data_count = 0
    # Process each JSON object
    for json_obj in json_objects:
        data = json.loads(json_obj)
        # print(data)
        if data['itemCount'] > 0:
            manifest_chunks.append(data)
            data_count = data_count + data['itemCount']

    # TODO Add progress bar

    print(f'Total items to import: {data_count}')
    process_json_objects(manifest_chunks, args)
    print("End Time:", datetime.datetime.now())


if __name__ == "__main__":
    main()
