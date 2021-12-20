import argparse
import time


class TestManager:
    def __init__(self, initial: int):
        self.ids = []
        self.initial = initial
        self.generate_ids()
        self.results = dict()
        for _id in ids:
            # assume all quality objectives to be unsatisfied (guilty until proven innocent)
            self.results.update({_id:
                {
                    "R01": False,
                    "R02": False,
                    "K01": False,
                    "K02": False,
                    "K03": False,
                    "K04": False,
                    "K05": False,
                    "K06": False,
                }})

    def generate_ids(self) -> None:
        for i in range(0, args['load']):
            _id = initial + len(ids)
            self.ids.append(_id)

    def generate_csv(self, outfile: str) -> None:
        pass
        # TODO implement


parser = argparse.ArgumentParser(description="Stress test the cluster and returns some metrics")
# RabbitMQ
parser.add_argument("host", type=str, help="Hostname or IP address of the broker")
parser.add_argument("password", type=str, help="Broker password")
parser.add_argument("user", type=str, help="Broker username")
parser.add_argument("--queue-name", "-qn", type=str, default="task", help="Queue name to publish to")
parser.add_argument("--port", "-p", type=int, default=30138, help="Host port for the broker")
parser.add_argument("--load", "-l", type=int, default=100, help="Amount of tasks to be added to the queue")
parser.add_argument("--outfile", "-o", type=str, default="out.csv", help="Output file for the results")
parser.add_argument("--runtime", "-t", type=int, help="Runtime in minutes before aggregating results")
parser.add_argument("--start-id", "-id", type=int, default=69_420,
                    help="Indicate id for first task object, will increment for all subsequent subtasks")

args = parser.parse_args()
testManager = TestManager(args['start_id'])
rabbitmq = RabbitMQ(args, testManager)
kubernetes = Kubernetes(args, testManager)
rabbitmq.do_publish()
time.sleep(args['runtime'] * 60)
rabbitmq.do_tests()
kubernetes.do_tests()
testManager.generate_csv(args['out'])