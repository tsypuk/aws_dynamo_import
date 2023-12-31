# Import into existing DynamoDB Table from AWS DynamoDB Export located in S3

## Problem statement

AWS provides DynamoDB Table export/import functionality.
According to aws docs (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.HowItWorks.html):

> **Note**
> Your data will be imported into a new DynamoDB table, which will be created when you initiate the import request. You can create this table with secondary indexes, then query and update your data across all primary and secondary indexes as soon as the import is complete. You can also add a global table replica after the import is complete.

However, it does not support LSI. The target table is created by AWS Export will be created *WITHOUT* LSI. 
Since LSIs can be created only during table setup time, and there is no control on Index of AWS Export - this creates a big problem.

**Data Access patterns will differ or will break the app logic.**

## Intention

This tool allows to use proprietary AWS DynamoDB Exports (located on S3) and import them into new table or existing one (with previously created configurations, LSI, GSI, etc).

## Supported DynamoDB export formats

- json.gz (dumps are extracted/unpackaged from gz)

## Execution flow

1. Launch AWS Data Export from your source table
2. Create new Table using cli, IaC or AWS console with all desired indexes
3. Run script and provide configuration in parameters
```shell
python main.py --export=01689247414762-5086b7df --bucket=ddb-export --table=target-table
```
4. Your target table will be populated from DataDump
5. Check the output with Details:

```shell
Start Time: 2023-07-13 20:23:20.260872

    S3 bucket with export: ddb-export
    S3 SSE algorithm: AES256
    export output format: DYNAMODB_JSON
    
    Export ARN: arn:aws:dynamodb:eu-west-1:123456789:table/source-table-name/export/01689247414762-5086b7df
    Export duration: 2023-07-13T11:23:34.762Z - 2023-07-13T11:28:43.215Z
    Export execution time: 2023-07-13T11:23:34.762Z
    
    Source Exported Table: arn:aws:dynamodb:eu-west-1:123456789:table/source-table-name
    Items count: 4205
    
Items count calculated in export chunks: 4205

Processing 1007 items from AWSDynamoDB/01689247414762-5086b7df/data/4knr5vr7ta7xzjdmf5wtstymsm.json.gz
Processing 1150 items from AWSDynamoDB/01689247414762-5086b7df/data/fcyaiq4ysazohblxp32pzf2w7q.json.gz
Processing 1019 items from AWSDynamoDB/01689247414762-5086b7df/data/doh7szv44m7z3iubuurbtrlsre.json.gz
Processing 1029 items from AWSDynamoDB/01689247414762-5086b7df/data/ajb3rxoet42ivlk4t664t27die.json.gz
100%|██████████| 1029/1029 [01:02<00:00, 16.42it/s]
100%|██████████| 1019/1019 [01:05<00:00, 15.60it/s]
100%|██████████| 1007/1007 [01:05<00:00, 15.32it/s]
100%|██████████| 1150/1150 [01:13<00:00, 15.65it/s]
End Time: 2023-07-13 20:27:17.165702
```

### Parallel execution
To speed up the process, some optimization are applied:
- additionally implemented multiprocessing of data ingestion (as was discovered AWS stores dump in multiple json.gz file, so they can be processed in parallel).
- before data ingestion *WCU* are increased not to throttle the parallel rate writes

## Parameters description

- export: identifier of export that was performed using DynamoDB export
- table: target table name where to import the Data
- bucket: bucket key where import is located
- region: AWS region for boto3
- pool: size on the pool of multiprocessing executors (each is processing partition in parallel to others). Default value is 1 - single thread sequential execution partition by partition. 

## Managing dependencies with Poetry

```shell
poetry install
poetry run python main.py             
```

## Managing dependencies with PIP

```shell
pip install -r requirements.txt
```
