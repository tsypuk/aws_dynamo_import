import boto3
import json
import gzip
import argparse
import datetime

# DynamoDB target table (should be created already)
wcu_import = 100
rcu_import = 1
wcu_post_import = 1
rcu_post_import = 1


def process_json_objects(session, json_objects, args):
    s3_client = session.client('s3')
    dynamodb_client = session.client('dynamodb')

    # Set higher WCU for faster Import
    dynamodb_client.update_table(
        TableName=args.table,
        ProvisionedThroughput={
            'ReadCapacityUnits': rcu_import,
            'WriteCapacityUnits': wcu_import
        }
    )

    for json_object in json_objects:
        # Get the S3 JSON.gz file path from the JSON object
        print(f"Processing {json_object['dataFileS3Key']}")

        # Read the JSON.gz file from the S3 bucket
        response = s3_client.get_object(Bucket=args.bucket, Key=json_object['dataFileS3Key'])
        gzipped_data = response['Body'].read()

        # Decompress the gzipped data
        json_data = gzip.decompress(gzipped_data).decode('utf-8')

        # Parse the JSON data
        json_data = json_data.strip().split('\n')
        json_items = []
        for item in json_data:
            json_items.append(json.loads(item))

        for item in json_items:
            dynamodb_client.put_item(
                TableName=args.table,
                Item=item['Item']
            )
            print(item['Item'])

    # Set the Read/Write Capacity Units post import completed
    dynamodb_client.update_table(
        TableName=args.table,
        ProvisionedThroughput={
            'ReadCapacityUnits': rcu_post_import,
            'WriteCapacityUnits': wcu_post_import
        }
    )


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

    # TODO Add progress bar
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
        print(data)
        if data['itemCount'] > 0:
            manifest_chunks.append(data)
            data_count = data_count + data['itemCount']

    print(f'Total items to import: {data_count}')
    process_json_objects(session, manifest_chunks, args)
    print("End Time:", datetime.datetime.now())


if __name__ == "__main__":
    main()
