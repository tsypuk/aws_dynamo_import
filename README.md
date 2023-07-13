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
5. Check the output with Details:

```shell
Start Time: 2023-07-13 20:23:20.260872
Total items to import: 4205
Processing AWSDynamoDB/01689247414762-5086b7df/data/4knr5vr7ta7xzjdmf5wtstymsm.json.gz partition with 1007 items.
100%|██████████| 1007/1007 [00:52<00:00, 19.13it/s]
Processing AWSDynamoDB/01689247414762-5086b7df/data/doh7szv44m7z3iubuurbtrlsre.json.gz partition with 1019 items.
100%|██████████| 1019/1019 [00:58<00:00, 17.47it/s]
Processing AWSDynamoDB/01689247414762-5086b7df/data/ajb3rxoet42ivlk4t664t27die.json.gz partition with 1029 items.
100%|██████████| 1029/1029 [00:55<00:00, 18.50it/s]
Processing AWSDynamoDB/01689247414762-5086b7df/data/fcyaiq4ysazohblxp32pzf2w7q.json.gz partition with 1150 items.
100%|██████████| 1150/1150 [01:06<00:00, 17.24it/s]
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

## Managing dependencies with Poetry

```shell
poetry install
poetry run python main.py             
```

## Managing dependencies with PIP

```shell
pip install -r requirements.txt
```
