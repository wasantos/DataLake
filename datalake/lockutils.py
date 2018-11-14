import boto3
import uuid

from datetime import datetime, timedelta

class LockerClient():
    def __init__(self, lockTableName):
        self.lockTableName = lockTableName
        self.db = boto3.client('dynamodb')

    def get_lock(self, lockName, expiresOn=None):
        now = datetime.utcnow()

        # Set TTL for lock (6 hours from execution time)
        ttl = now + timedelta(hours=6)

        # Set default value for expiresOn
        if not expiresOn:
            expiresOn = ttl

        # First get the row for 'name'
        get_item_params = {
            'TableName': self.lockTableName,
            'Key': {
                'name': {
                    'S': lockName,
                }
            },
            'AttributesToGet': [
                'requestedAt', 'expiresOn', 'guid'
            ],
            'ConsistentRead': True,
        }

        put_item_params = {
            'Item': {
                'name': {
                    'S': lockName
                },
                'requestedAt': {
                    'S': now.isoformat()
                },
                'guid': {
                    'S': str(uuid.uuid4())
                },
                'expiresOn': {
                    'N': str(expiresOn.timestamp())
                },
                'ttl': {
                    'N': str(ttl.timestamp())
                }
            },
            'TableName': self.lockTableName
        }

        try:
            data = self.db.get_item(**get_item_params)

            if 'Item' not in data:
                # Table exists, but lock not found. We'll try to add a lock
                # If by the time we try to add we find that the attribute guid exists (because another client grabbed it), the lock will not be added
                put_item_params['ConditionExpression'] = 'attribute_not_exists(guid)'

            # We know there's possibly a lock'. Check to see it's expired yet
            elif float(data['Item']['expiresOn']['N']) > now.timestamp():
                return False
            else:
                # We know there's possibly a lock and it's expired. We'll take over.
                # This is an atomic conditional update
                print("Expired lock found. Attempting to aquire")
        except Exception as e:
            print("Exception" + str(e))
            # Something nasty happened. Possibly table not found
            return False

        # now we're going to try to get the lock. If ANY exception happens, we assume no lock
        try:
            self.db.put_item(**put_item_params)
            return True
        except Exception:
            return False

    def release_lock(self, lockName):
        delete_item_params = {
            'Key': {
                'name': {
                    'S': lockName,
                }
            },
            'TableName': self.lockTableName
        }

        try:
            self.db.delete_item(**delete_item_params)
        except Exception as e:
            print(str(e))
