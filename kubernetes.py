import traceback

import kubernetes
from kubernetes import config, client
from stress import TestManager


class Kubernetes:
    def __init__(self, test_manager: TestManager):
        config.load_kube_config()
        self.test_manager = test_manager
        self.client = client.CoreV1Api()

    def do_tests(self) -> None:
        # [K01] A scheduler is created in the system.
        # [K05] The scheduler shuts itself down after it has finished its task
        self.k01_05()
        # [K02] The investigation subtask is started
        # [K03] The calculate subtask(s) is/are started
        # [K04] The conclude subtask is started
        self.k02_03_04_06()

    def k01_05(self) -> None:
        pods = self.client.list_namespaced_pod('schedulers').items
        # check which schedulers are present
        for pod in pods:
            _id = int(pod.metadata.name.split("scheduler-")[0])
            # if it exists k01 is satisfied
            self.test_manager.results[_id]['K01'] = True
            # check for k05
            self.test_manager.results[_id]['K05'] = pod.status.phase == "Succeeded"
            # TODO has been wonky in the past, check if it works as intended (the succeed status phase)

    def k02_03_04_06(self) -> None:
        for _id in self.test_manager.results.keys():
            pods = []
            try:
                pods = self.client.list_namespaced_pod(f'subtask-{_id}')
            except:
                # likely means the namespace isn't present so may as well break the loop iteration
                print(f"namespace not found, trace:\n")
                traceback.print_exc()
                continue
            # if there is >=1 pod, k02 is likely satisfied
            # TODO K02, K03, K04, K06
