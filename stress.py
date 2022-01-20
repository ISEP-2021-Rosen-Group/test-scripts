import argparse
import csv
import time

from kube import Kubernetes
from rabbitmq import RabbitMQ


class TestManager:
    def __init__(self, args):
        self.ids = []
        self.args = args
        self.generate_ids()
        self.results = dict()
        for _id in self.ids:
            # assume all quality objectives to be unsatisfied (guilty until proven innocent)
            self.results.update({_id:
                {
                    "R01": True,  # task is consumed unless we spot it on the queue
                    "R02": False,
                    "K01": False,
                    "K02": False,
                    "K03": False,
                    "K04": False,
                    "K05": False,
                    "K06": True,  # task is finished unless we spot it Running
                }})

    def generate_ids(self) -> None:
        for i in range(0, self.args.load):
            _id = self.args.start_id + len(self.ids)
            self.ids.append(_id)

    def generate_csv(self, outfile: str) -> None:
        with open(outfile, "w") as file:
            file.write('id,R01,R02,K01,K02,K03,K04,K05,K06\n')
            w = csv.DictWriter(file, ['id'] + list(self.results[self.args.start_id].keys()))
            for key, val in self.results.items():
                row = {'id': key}
                row.update(val)
                w.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="Stress test the cluster and returns some metrics")
    # RabbitMQ
    parser.add_argument("--host", type=str, default='utwente-isep2021.northeurope.cloudapp.azure.com',
                        help="Hostname or IP address of the broker")
    parser.add_argument("--user", type=str, default='ISEPRosen', help="Broker username")
    parser.add_argument("--password", type=str, default='Rosen_Twente_2021', help="Broker password")
    parser.add_argument("--queue-name", "-qn", type=str, default="task", help="Queue name to publish to")
    parser.add_argument("--port", "-p", type=int, default=30138, help="Host port for the broker")
    parser.add_argument("--api-port", "-ap", type=int, default=30140, help="Host port for the broker http api")
    parser.add_argument("--load", "-l", type=int, default=20, help="Amount of tasks to be added to the queue")
    parser.add_argument("--outfile", "-o", type=str, default="out.csv", help="Output file for the results")
    parser.add_argument("--start-id", "-id", type=int, default=69_420,
                        help="Indicate id for first task object, will increment for all subsequent subtasks")

    args = parser.parse_args()
    test_manager = TestManager(args)
    rabbitmq = RabbitMQ(args, test_manager)
    kubernetes = Kubernetes(test_manager)
    rabbitmq.do_publish()
    # time.sleep(args.runtime * 60)
    print(f"{args.load} tasks scheduled")
    input("Press enter to collect results...\n")
    rabbitmq.do_tests()
    kubernetes.do_tests()
    rabbitmq.cleanup()
    kubernetes.cleanup()
    test_manager.generate_csv(args.outfile)


if __name__ == '__main__':
    main()
