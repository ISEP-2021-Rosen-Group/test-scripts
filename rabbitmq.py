import json
import threading
import time
from datetime import datetime

import pika


class RabbitMQ:

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

    # TODO R01, R02
