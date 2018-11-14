import boto3
import requests
import logging
import json
import os
from requests_aws4auth import AWS4Auth

# Setup endpoints
ES_ENDPOINT = os.environ['es_endpoint']
ES_DOMAIN = os.environ['domain_name']
url = f'https://{ES_ENDPOINT}/{ES_DOMAIN}/{ES_DOMAIN}'

# Setup credentials
ES_REGION = os.environ['region']
def setup_credentials():
    credentials = boto3.Session().get_credentials()
    return AWS4Auth(credentials.access_key, credentials.secret_key,
        ES_REGION, 'es', session_token=credentials.token)

# Setup headers
headers = { "Content-Type": "application/json" }

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info("Received event, handling for ES")

    for record in event['Records']:
        payload = json.loads(record['Sns']['Message'])
        
        LOGGER.info("Adding ingestion log entry")
        response = requests.post(url, timeout=(5, 5),
            auth=setup_credentials(), json=payload, headers=headers)
        if not response.ok:
            LOGGER.error("Error sending data to ES (%s): %s",
                str(response.status_code), str(response.json()))

def purge(event, context):
    LOGGER.info("Received purge event, ignoring payload")

    query_payload = {
        "query": {
            "range" : {
                "event_timestamp" : {
                    "lt" : "now-14d/d"
                }
            }
        }
    }

    # delete_by_query can be slow, be prepared for it
    response = requests.post(url + '/_delete_by_query', timeout=(5, 60),
        auth=setup_credentials(), json=query_payload, headers=headers)
    if not response.ok:
        LOGGER.error("Error purging old data from ES (%s): %s",
            str(response.status_code), str(response.json()))
