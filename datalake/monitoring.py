import boto3
import json
import traceback
import os

from pyutils import pick
from datetime import datetime

class MonitoringSettings:
    global_settings = None
    topic_factory = lambda: os.environ.get('sns_monitoring_topic', None)

    __slots__ = ['monitoring_topic']

    @classmethod
    def get_global(cls):
        if cls.global_settings is None:
            cls.global_settings = cls(monitoring_topic=cls.topic_factory())
        
        return cls.global_settings

    def __init__(self, monitoring_topic=None):
        self.monitoring_topic = monitoring_topic

class Task:
    __partition_fields = ["country", "year", "month", "day", "secs"]
    __current = None

    @classmethod
    def current(cls):
        return cls.__current
    
    @classmethod
    def track(cls, system: str, **params):
        cls.__current = cls(system, params)

    def __init__(self, system: str, params: dict):
        settings = params.get('settings')
        if params.get('settings') is None:
            settings = MonitoringSettings.get_global()

        if settings.monitoring_topic is None:
            self.sns = None
            self.topic = None
            return

        self.system = system
        self.params = params
        self.sns = boto3.resource('sns')
        self.topic = self.sns.Topic(settings.monitoring_topic)
        self.execution_id = self.get_execution_id()
        self.attempt = self.params.get('attempt', 0)
        self.flow_id = self.get_flow_id()
        self.id = f"{self.system}:{self.flow_id}:{self.execution_id}"

    def get_flow_id(self):
        columns = self.__partition_fields
        return "/".join(map(lambda col: f"{col}={self.params[col]}", columns))

    def get_execution_id(self):
        if self.params.get("execution_dt"):
            return str(int(self.params.get("execution_dt").timestamp() * 1000))
        elif self.params.get("execution_id"):
            return self.params.get("execution_id")
        else:
            raise ValueError("no execution date or execution id supplied")

    def as_dict(self):
        base = pick(self.params, *self.__partition_fields)
        base['execution_id'] = self.execution_id
        base['attempt'] = self.attempt
        base['system'] = self.system
        return base

    def __event(self, status: str, ok: bool, payload: dict):
        if self.topic is not None:
            try:
                message = {}
                message['id'] = self.id
                message['status'] = status
                message['ok'] = ok
                message['timestamp'] = datetime.utcnow().isoformat()
                message['system'] = self.system
                message['partitions'] = pick(self.params, *self.__partition_fields)
                message['payload'] = payload

                self.topic.publish(Message=json.dumps(message))
            except Exception:
                print("Monitoring failed")
                traceback.print_exc()

    def success(self, status: str, **payload):
        self.__event(status, True, payload)
    
    def failure(self, status: str, **payload):
        self.__event(status, False, payload)
