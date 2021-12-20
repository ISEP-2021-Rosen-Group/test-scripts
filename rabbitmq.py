import threading
import time

import pika


class RabbitMQ:

    def __init__(self, host='utwente-isep2021.northeurope.cloudapp.azure.com', port=30138, username='ISEPRosen',
                 password='Rosen_Twente_2021'):
        super(RabbitMQ, self).__init__()
        self.rabbitmq_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port,
                                      heartbeat=30,
                                      blocked_connection_timeout=60,
                                      credentials=pika.PlainCredentials(username=username,
                                                                        password=password)))
        self.heartbeat_thread = threading.Thread(target=self.do_heartbeat, args=(self.rabbitmq_connection,))
        self.heartbeat_thread.setDaemon(True)
        self.heartbeat_thread.start()

    def do_heartbeat(self, connection):
        while True:
            connection.process_data_events()
            time.sleep(30)

    def basic_publish(self, queue_name, message):
        channel = self.rabbitmq_connection.channel()
        channel.basic_publish(exchange='', routing_key=queue_name, body=message)
        channel.close()

    def get_message_count(self, queue_name):
        channel = self.rabbitmq_connection.channel()
        queue = channel.queue_declare(queue_name, passive=True)
        channel.close()
        return queue.method.message_count

    def create_queue(self, queue_name):
        channel = self.rabbitmq_connection.channel()
        queue = self.rabbitmq_channel.queue_declare(queue_name)
        channel.close()
        return queue

    #TODO R01, R02








