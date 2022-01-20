import http
import json
import threading
import time
from base64 import b64encode
from datetime import datetime
from http.client import HTTPConnection

import pika


class RabbitMQ:
    class RESTHandler:
        def __init__(self, user, password, host, port):
            self.user = user
            self.password = password
            self.host = host
            self.port = port

        def get_queue_names(self) -> set[str]:
            user_and_pass = b64encode(bytes(f"{self.user}:{self.password}", "utf-8")).decode("ascii")
            headers = {'Authorization': 'Basic %s' % user_and_pass}
            conn = HTTPConnection(f"{self.host}:{self.port}")
            conn.request("GET", "/api/queues", headers=headers)
            response = json.loads(conn.getresponse().read())
            return set(map(lambda x: x['name'], response))

    def __init__(self, args, test_manager):
        super(RabbitMQ, self).__init__()
        self.args = args
        self.test_manager = test_manager
        self.rabbitmq_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=args.host, port=args.port,
                                      heartbeat=30,
                                      blocked_connection_timeout=60,
                                      credentials=pika.PlainCredentials(username=args.user,
                                                                        password=args.password)))
        self.heartbeat_thread = threading.Thread(target=self.do_heartbeat, args=(self.rabbitmq_connection,))
        self.heartbeat_thread.setDaemon(True)
        self.heartbeat_thread.start()

    def do_heartbeat(self, connection):
        while True:
            connection.process_data_events()
            time.sleep(30)

    def basic_publish(self, queue_name, message, channel=None):
        if channel is None:
            channel = self.rabbitmq_connection.channel()
            channel.basic_publish(exchange='', routing_key=queue_name, body=message)
            channel.close()
        else:
            channel.basic_publish(exchange='', routing_key=queue_name, body=message)

    def get_message_count(self, queue_name, channel=None):
        if channel is None:
            channel = self.rabbitmq_connection.channel()
            queue = channel.queue_declare(queue_name, passive=True)
            channel.close()
        else:
            queue = channel.queue_declare(queue_name, passive=True)
        return queue.method.message_count

    def create_queue(self, queue_name, channel=None):
        if channel is None:
            channel = self.rabbitmq_connection.channel()
            queue = channel.queue_declare(queue_name, durable=True)
            channel.close()
        else:
            queue = channel.queue_declare(queue_name, durable=True)
        return queue

    def do_publish(self):
        channel = self.rabbitmq_connection.channel()
        self.create_queue(self.args.queue_name, channel=channel)
        for _id in self.test_manager.ids:
            self.basic_publish(self.args.queue_name, json.dumps({
                "type": "task-submit",
                "id": _id,
                "createdAt": datetime.now().isoformat(),
                "name": f"test-{_id}",
                "dockerImage": "rosenisep/rosen-processing",
                "dockerImageTag": "latest",
                "subTasks": [
                    {"operation": "investigate", "arguments": {"--infile": "input.txt"}},
                    {"operation": "calculate", "arguments": {}},
                    {"operation": "conclude", "arguments": {"--outfile": "output.txt"}}
                ]}), channel=channel)

    def get_all_unconsumed_tasks(self):
        channel = self.rabbitmq_connection.channel()
        messages_available = True
        frames = set()
        messages = set()
        while messages_available:
            mf, hf, body = channel.basic_get(self.args.queue_name, auto_ack=False)
            if mf:
                frames.add(mf)
                messages.add(body)
            else:
                messages_available = False
        map(lambda frame: channel.basic_nack(frame.delivery_tag), frames)
        channel.close()
        return messages

    def do_tests(self):
        unconsumed_task_ids = set(map(lambda task: int(json.loads(task)["id"]), self.get_all_unconsumed_tasks()))
        queue_names = RabbitMQ.RESTHandler(
            user=self.args.user,
            password=self.args.password,
            port=self.args.api_port,
            host=self.args.host
        ).get_queue_names()
        for _id in self.test_manager.results.keys():
            # check for r01
            if _id in unconsumed_task_ids:
                self.test_manager.results[_id]['R01'] = False
            # check for r02
            if f"subtasks-com-dev-{_id}" in queue_names and f"subtasks-dev-{_id}" in queue_names:
                self.test_manager.results[_id]['R02'] = True

    def cleanup(self):
        # delete all unconsumed tasks
        channel = self.rabbitmq_connection.channel()
        messages_available = True
        frames_to_ack = set()
        frames_to_nack = set()
        while messages_available:
            mf, hf, body = channel.basic_get(self.args.queue_name, auto_ack=False)
            if mf:
                _id = int(json.loads(body)["id"])
                if _id in self.test_manager.results.keys():
                    frames_to_ack.add(mf)
                else:
                    frames_to_nack.add(mf)
            else:
                messages_available = False
        map(lambda frame: channel.basic_nack(frame.delivery_tag), frames_to_nack)
        map(lambda frame: channel.basic_ack(frame.delivery_tag), frames_to_ack)
        # delete queues
        for _id in self.test_manager.results.keys():
            channel.queue_delete(f"subtasks-com-dev-{_id}")
            channel.queue_delete(f"subtasks-dev-{_id}")
        channel.close()
