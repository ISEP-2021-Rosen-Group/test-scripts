import traceback

from kubernetes import config, client
from kubernetes.client import ApiException


class Kubernetes:
    def __init__(self, test_manager):
        config.load_kube_config()
        self.test_manager = test_manager
        self.client = client.CoreV1Api()

    def do_tests(self) -> None:
        # [K01] A scheduler is created in the system.
        # [K05] The scheduler shuts itself down after it has finished its task
        self._k01_05()
        # [K02] The investigation subtask is started
        # [K03] The calculate subtask(s) is/are started
        # [K04] The conclude subtask is started
        self._k02_03_04_06()

    def _k01_05(self) -> None:
        pods = self.client.list_namespaced_pod('schedulers').items
        # check which schedulers are present
        for pod in pods:
            _id = int(pod.metadata.name.split("scheduler-")[1])
            # if it exists k01 is satisfied
            self.test_manager.results[_id]['K01'] = True
            # check for k05
            self.test_manager.results[_id]['K05'] = pod.status.phase == "Succeeded"

    def _k02_03_04_06(self) -> None:
        for _id in self.test_manager.results.keys():
            pods = []
            try:
                pods = self.client.list_namespaced_pod(f'subtask-{_id}').items
            except:
                # likely means the namespace isn't present so may as well break the loop iteration
                print(f"namespace not found, trace:\n")
                traceback.print_exc()
                continue
            # for all pods in the name space check:
            # [K02] check logs for keyword "Investigate"
            # [K03] check logs for keyword "Calculate"
            # [K04] check logs for keyword "Conclude"
            for pod in pods:
                logs = self.client.read_namespaced_pod_log(name=pod.metadata.name, namespace=f'subtask-{_id}')
                if "Investigate" in logs:
                    self.test_manager.results[_id]['K02'] = True
                if "Calculate" in logs:
                    self.test_manager.results[_id]['K03'] = True
                if "Conclude" in logs:
                    self.test_manager.results[_id]['K04'] = True
                if pod.status.phase == "Running":
                    self.test_manager.results[_id]['K06'] = False

    def cleanup(self):
        for _id in self.test_manager.ids:
            try:
                # delete scheduler
                self.client.delete_namespaced_pod(f'scheduler-{_id}', 'schedulers')
            except ApiException as e:
                print(f"can't find scheduler-{_id}, assuming it never got created, continuing...")
            try:
                # ...and delete associated subtask namespace
                self.client.delete_namespace(f'subtask-{_id}')
            except ApiException as e:
                print(f"can't find namespace subtask-{_id}, assuming it never got created, continuing...")

