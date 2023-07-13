# Import into existing table from AWS DynamoDB Export

## Problem statement

AWS provides DynamoDB Table export/import functionality, according to documentation(https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.HowItWorks.html):

> **Note**
> Your data will be imported into a new DynamoDB table, which will be created when you initiate the import request. You can create this table with secondary indexes, then query and update your data across all primary and secondary indexes as soon as the import is complete. You can also add a global table replica after the import is complete.

However, it does not support LSI. The target table where data is exported will be created *WITHOUT* LSI. This creates a problem.

## Intention

This tool allow to use AWS DynamoDB Exports and import them into new table with previously created LSI, GSI, etc.
The flow is:
1. Launch AWS Data Export from your source table
2. Create new Table using cli, IaC or AWS console with all desired indexes
3. Run script and provide configuration in parameters
```shell
python main.py --export=01689247414762-5086b7df --bucket=ddb-export --table=target-table
```
4. Your target table will be populated from DataDump

### Parallel execution
To speed up the process, some optimization are applied:
- additionally implemented multiprocessing of data ingestion (as was discovered AWS stores dump in multiple json.gz file, so they can be processed in parallel).
- before data ingestion *WCU* are increased not to throttle the parallel rate writes

## Parameters description

- export: identifier of export that was performed using DynamoDB export
- table: target table name where to import the Data
- bucket: bucket key where import is located
- region: AWS region for boto3
