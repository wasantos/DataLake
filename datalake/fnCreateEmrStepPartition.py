import boto3
import os
import dlutils
import json
import uuid

from lockutils import LockerClient
from pyutils import EnvObject

sns = boto3.client('sns')
sqs = boto3.resource('sqs')
fnc = boto3.client('lambda')

env = EnvObject()
map_steps = dlutils.map_steps

def continue_step_chain(event):
    step = event['Step']
    task = event['Task']
    status = event['Status']

    next_attempt = task['attempt'] + 1
    next_step = map_steps[step]
    locker = LockerClient(env.lock_table)
    
    # Unlock country, restart cycle
    if next_step == 'final' and status == 'COMPLETED':
        locker.release_lock(f"emrsteps:main:{task['country']}")
        fnc.invoke(FunctionName=env.function_name,
            InvocationType='Event',
            Payload=json.dumps({
                'Step': 'initial',
                'Sentinel': str(uuid.uuid4())
            }).encode())
    # Otherwise continue chain
    else:
        print('Process next Step')

        # Job succesful, next step in the chain
        if status == 'COMPLETED':
            dlutils.send_job(next_step, task)
        # Job failed, can be retried
        elif status == 'FAILED' and next_attempt <= 5:
            task['attempt'] = next_attempt
            print('Invoke: ' + step)
            dlutils.send_job(step, task)
        # Job failed, can't be retried, go to dead letter queue
        else:
            queue_name = dlutils.get_queue_name(env.sqs_dead_prefix,
                env.env, env.project, fifo=False)
            queue = sqs.get_queue_by_name(QueueName=queue_name)
            queue.send_message(MessageBody=json.dumps(task))

            # Restart cycle
            locker.release_lock(f"emrsteps:main:{task['country']}")
            fnc.invoke(FunctionName=env.function_name,
                InvocationType='Event',
                Payload=json.dumps({
                    'Step': 'initial',
                    'Sentinel': str(uuid.uuid4())
                }).encode())

def lambda_handler(event, context):
    print('Start: ' + str(event))
    if isinstance(event, str):
        event = json.loads(event)

    try:
        sentinel = event['Sentinel']
        print(f"sentinel: {sentinel}")

        # We need to handle multiple lambda calls because AWS sucks
        locker = LockerClient(env.lock_table)

        if not locker.get_lock(f"emrsteps:sentinel:{sentinel}"):
            # This request is a repeat, just stop the lambda
            # The sentinel is a random UUID, so there isn't risk
            # of collisions - we let AWS handle the deletion
            # through TTL
            return

        # Called back from EMR
        if event['Step'] != 'initial':
            continue_step_chain(event)
        # Else, start chain from zero
        else:
            queue_name = dlutils.get_queue_name(env.sqs_prefix, env.env, env.project)
            queue = sqs.get_queue_by_name(QueueName=queue_name)
            attempts = 0

            # Try for each country, at most
            while attempts < 12:
                messages = queue.receive_messages(
                    AttributeNames=['MessageGroupId'],
                    MaxNumberOfMessages=1)
                
                # Exit early if no messages in queue
                if not messages:
                    return
                
                message = messages[0]
                country = message.attributes['MessageGroupId']

                # Check if country is being processed
                if not locker.get_lock(f"emrsteps:main:{country}"):
                    # return message to queue
                    message.change_visibility(VisibilityTimeout=0)
                    # try again
                    attempts += 1
                    continue

                # We got the lock, start processing
                payload = json.loads(message.body)
                next_step = map_steps['initial']
                dlutils.send_job(next_step, payload)
                message.delete()

                # We can quit now
                return
    except Exception as e:
        sns.publish(TopicArn=env.sns_topic,
            Message=f"Error executing Lambda {env.function_name}.")
        raise e from None
