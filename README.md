# test-scripts
## What does it do?
Testing for the following on a specified number of tasks:
- \[R01\] The task is consumed by the designated scheduler
- \[R02\] Two message queues are created for the scheduler
- \[K01\] A scheduler is created in the system.
- \[K02\] The investigation subtask is started
- \[K03\] The calculate subtask(s) is/are started
- \[K04\] The conclude subtask is started
- \[K05\] The scheduler shuts itself down after it has finished its task
- \[K06\] No unfinished subtasks remain in the system

## Usage
```
$ python stress.py -h
usage: stress.py [-h] [--queue-name QUEUE_NAME] [--port PORT] [--load LOAD]
                 [--outfile OUTFILE] [--runtime RUNTIME] [--start-id START_ID]
                 host password user

Stress test the cluster and returns some metrics

positional arguments:
  host                  Hostname or IP address of the broker
  password              Broker password
  user                  Broker username

optional arguments:
  -h, --help            show this help message and exit
  --queue-name QUEUE_NAME, -qn QUEUE_NAME
                        Queue name to publish to
  --port PORT, -p PORT  Host port for the broker
  --load LOAD, -l LOAD  Amount of tasks to be added to the queue
  --outfile OUTFILE, -o OUTFILE
                        Output file for the results
  --runtime RUNTIME, -t RUNTIME
                        Runtime in minutes before aggregating results
  --start-id START_ID, -id START_ID
                        Indicate id for first task object, will increment for
                        all subsequent subtasks
```