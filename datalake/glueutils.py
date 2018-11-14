# Utilities for handling Glue
import datetime
import boto3
import dlutils
from typing import List, Dict

GLUE_TABLE_FORMATS = {
    'csv': {
        'Input': 'org.apache.hadoop.mapred.TextInputFormat',
        'Output': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        'Serde': {
            'Lib': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
            'Params': {
                'field.delim': '\t'
            }
        },
        'Prefix': '',
        'Key': lambda t, env: f"{env['glue_s3_key']}/csv/{t}",
        'Bucket': lambda env: env['bucket_output']
    },
    'parquet': {
        'Input': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'Output': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'Serde': {
            'Lib': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'Params': {
                'serialization.format': '1'
            }
        },
        'Prefix': 'tbpq_',
        'Key': lambda t, env: f"{env['glue_s3_key']}/parquet/{t}",
        'Bucket': lambda env: env['bucket_output']
    }
}

DEFAULT_PARTITION_KEYS = [
    {'Name': 'pt_country', 'Type': 'string'},
    {'Name': 'pt_year', 'Type': 'string'},
    {'Name': 'pt_month', 'Type': 'string'},
    {'Name': 'pt_day', 'Type': 'string'},
    {'Name': 'pt_secs', 'Type': 'string'}
]

def table_spec(table_name: str, kind: str, s3_path: str, columns: List[str]):
    """
    Returns a valid table spec for use with Glue CreateTable API
    """

    formats = GLUE_TABLE_FORMATS[kind]

    return {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Compressed': True,
            'Location': s3_path,
            'InputFormat': formats['Input'],
            'OutputFormat': formats['Output'],
            'SerdeInfo': {
                'SerializationLibrary': formats['Serde']['Lib'],
                'Parameters': formats['Serde']['Params']
            }
        },
        'PartitionKeys': DEFAULT_PARTITION_KEYS,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE',
            'classification': kind,
            'creationDate': datetime.datetime.utcnow().isoformat()
        }
    }

def parse_fmt(data: str) -> List[Dict[str, str]]:
    """
    Parses a string representing a FMT into a column specification
    for use with `table_spec`
    """
    lines = data.splitlines()

    # Line 0 contains (possibly fake) SQL version string
    # Line 1 contains number of columns to handle
    column_count = int(lines[1])
    
    # Columns start from line 2
    start = 2
    end = start + column_count

    # Extract columns
    columns = [None] * column_count
    for column_line in lines[start:end]:
        position, *_ignored, name = column_line.split('\t')
        columns[int(position) - 1] = {'Name': name, 'Type': 'string'}

    return columns

def load_fmt(system: str, interface: str, env: Dict[str, str]) -> str:
    """
    Load a FMT file from S3.
    """

    s3 = boto3.resource('s3')
    key = f"{env['key_prefix_fmt']}/{system}/{interface}.fmt"
    print(f"Loading FMT {env['bucket_output']}/{key}")
    fmt = s3.Object(env['bucket_output'], f"{env['key_prefix_fmt']}/{system}/{interface}.fmt")
    return fmt.get()['Body'].read().decode()

def table_exists(database: str, name: str) -> bool:
    """
    Check if given table exists in the Glue catalog
    """

    glue = boto3.client('glue')
    resp = glue.get_tables(DatabaseName=database, Expression=f".*{name}.*")
    return len(resp['TableList']) > 0

def create_table(system: str, interface: str, kind: str, env: Dict[str, str]):
    """
    Create table for given system and interface, if it exists
    Return name and warehouse path info for the table
    """
    
    formats = GLUE_TABLE_FORMATS[kind]
    table_name = f"{formats['Prefix']}{system}_{interface}"
    bucket = formats['Bucket'](env)
    key = formats['Key'](table_name, env)
    s3_path = f"s3://{bucket}/{key}"

    if table_exists(env['glue_db'], table_name):
        return table_name, bucket, key

    columns = parse_fmt(load_fmt(system, interface, env))
    table = table_spec(table_name, kind, s3_path, columns)

    glue = boto3.client('glue')

    try:
        glue.create_table(DatabaseName=env['glue_db'], TableInput=table)
    except glue.exceptions.AlreadyExistsException:
        print(f"{table_name} already exists")

    return table_name, bucket, key
