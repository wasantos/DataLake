import zipfile
import boto3
import io
import datetime
import os
import random
import json
import dlutils
import glueutils
import json
import uuid

from monitoring import Task
from traceback import format_exc
from pyutils import EnvObject

sns = boto3.client('sns')
s3 = boto3.client('s3')
glue = boto3.client('glue')
fnc = boto3.client('lambda')
sqs = boto3.resource('sqs')
env = EnvObject()

# s3://belc-bigdata-landing-dlk-qas/datalake/input/sicc/co
level_system = 2
level_country = 3

interfaces = dlutils.interfaces

def lambda_handler(event, context):
    print('Start: ' + str(event))
    if isinstance(event, str):
        event = json.loads(event)
    
    now = datetime.datetime.utcnow()
    
    year = str(now.year)
    month = str(now.month)
    day = str(now.day)
    secs = str(now.hour * 24 * 60 + now.minute * 60 + now.second)
    key = event['Records'][0]['s3']['object']['key']
    print(key)
    print(now)

    system = key.split('/')[level_system]
    country = key.split('/')[level_country].upper()
    if len(country) > 2 or len(key.split('/')) != 5:
        raise ValueError(
            'Input path (S3 key) error. '
            f'Check if zip file is in {env.bucket_input}/zips/system/country/zipname.zip'
        )

    # Start tracking 
    Task.track(system, execution_dt=now,
        country=country, year=year, month=month, day=day, secs=secs)
    serialized_task = json.dumps(Task.current().as_dict())

    try:
        obj = s3.get_object(Bucket=env.bucket_input, Key=key)
        total_line_count = 0
        with io.BytesIO(obj["Body"].read()) as tf:
            tf.seek(0)
            # Read the file as a zipfile and process the members
            with zipfile.ZipFile(tf, mode='r') as zipf:
                for file in zipf.infolist():
                    name = file.filename.split('.')[0].lower()
                    if name == 'sap':
                        name = file.filename.split('.')[1].lower()
                    if name in interfaces[system]:
                        # Create CSV and Parquet tables
                        table_name, table_bucket, table_key = glueutils.create_table(system, name, 'csv', os.environ)
                        glueutils.create_table(system, name, 'parquet', os.environ)
                        
                        partition = 'pt_country=' + country + \
                                   '/pt_year=' + year + \
                                   '/pt_month=' + month + \
                                   '/pt_day=' + day + \
                                   '/pt_secs=' + secs

                        new_filename = f"{name}{str(int(now.timestamp()))}.txt"
                        fullpath = f"{table_key}/{partition}/{new_filename}"
                        filebody = zipf.read(file).decode('utf-8', errors='replace')
                        line_count = filebody.count("\r\n")
                        total_line_count += line_count
                        s3.put_object(Bucket=table_bucket, Key=fullpath,
                            Body=filebody.encode(encoding='utf-8'))
                        
                        Task.current().success("CSV_INGESTED",
                            line_count=line_count, interface=name, output=f"s3://{table_bucket}/{fullpath}")

                        table = dlutils.get_glue_table(glue, env.glue_db, table_name)
                        new_partition = {
                            'Values': [country, year, month, day, secs],
                            'StorageDescriptor': {
                                'OutputFormat': table['StorageDescriptor']['OutputFormat'],
                                'InputFormat': table['StorageDescriptor']['InputFormat'],
                                'SerdeInfo': table['StorageDescriptor']['SerdeInfo'],
                                'Columns': table['StorageDescriptor']['Columns'],
                                'Location': f"s3://{table_bucket}/{table_key}/{partition}"
                            }
                        }

                        queue_name = dlutils.get_queue_name(env.sqs_prefix, env.env, env.project)
                        queue = sqs.get_queue_by_name(QueueName=queue_name)
                        print(new_partition)
                        
                        try:
                            response = glue.create_partition(DatabaseName=env.glue_db,
                                                                TableName=table['Name'],
                                                                PartitionInput=new_partition)
                        except glue.exceptions.AlreadyExistsException as e:
                            print('AlreadyExistsException')
                        
                        try:
                            queue.send_message(MessageBody=serialized_task, MessageGroupId=country)
                        except KeyError:
                            print(response)

        fnc.invoke(FunctionName=env.function_process_partition,
            InvocationType='Event',
            Payload=json.dumps({
                'Step': 'initial',
                'Sentinel': str(uuid.uuid4())
            }).encode())

        Task.current().success("ZIP_INGESTED",
            line_count=total_line_count, interface=name)

    except Exception as e:
        # Log failure
        Task.current().failure("UNZIP_FAILED", exception_message=format_exc())
        
        # Print and stop lambda
        print(e)
        raise e from None

    sns.publish(TopicArn=env.sns_topic,
        Message=f'Lambda {serialized_task} executada com sucesso.')
