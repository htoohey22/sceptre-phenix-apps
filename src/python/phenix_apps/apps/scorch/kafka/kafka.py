import subprocess
import shlex
import json
import itertools
import threading
import time
import sys
import csv
import re
import os
import time
from phenix_apps.apps.scorch import ComponentBase
from phenix_apps.common import logger, utils
from kafka import KafkaConsumer
from kafka.errors import KafkaError

class Kafka(ComponentBase):
    def __init__(self):
        ComponentBase.__init__(self, 'kafka')
        self.pid_file = f"/tmp/kafka-{self.exp_name}.pid"
        self.execute_stage()
    
    #configure just calls start
    def configure(self):
        start()

    def start(self):
        self.scorch_kafka_running = True
        logger.log('INFO', f'Starting user component: {self.name}')

        #get all variables from tags
        kafka_ips = self.metadata.get("kafka_ips", ["172.20.0.63:9092"])
        topics = self.metadata.get("topics", [])
        csvBool = self.metadata.get("csv", True) #if false output a JSON

        #get and output the output directory to the logger
        output_dir = self.base_dir
        logger.log('INFO', f'Output Directory: {output_dir}')
        os.makedirs(output_dir, exist_ok=True)
        if csvBool:
            self.path = os.path.join(output_dir, f'kafka_{self.name}_output.csv')
        else:
            self.path = os.path.join(output_dir, f'kafka_{self.name}_output.json')


        kafka_ips_str = ",".join(kafka_ips)
        topics_str = json.dumps(topics)

        #pass the inputs to the python file (which we execute as a separate process)
        executable  = "/usr/local/lib/python3.12/dist-packages/phenix_apps/apps/scorch/kafka/scorch_kafka_listener.py"
        arguments = f"python3 {executable} {csvBool} '{self.path}' {kafka_ips_str} '{topics_str}'"
        command = shlex.split(arguments)

        try:
            log_file = open("/var/log/phenix/kafka-app.log", '+a')
            response = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log_file, stderr=log_file, start_new_session=True)
            self._create_pid_file(response.pid) #write PID to a file so that it can be found and killed later
            response.poll() #prevents hang
        except Exception as e:
            logger.log("ERROR", f"Error running listener executable. See: {e}")

    def _create_pid_file(self, pid):
        #writes PID to unique .pid file under /tmp directory
        try:
            with open(self.pid_file, "w+") as f:
                f.write(str(pid))
        except Exception as e:
            logger.log("ERROR", f"Could not create PID file. Listener process at PID {pid} will not terminate automatically when experiment ends. See: {e}")


    def _consume_pid_file(self):
        #reads and deletes PID file, returns PID
        pid = 0
        try: 
            with open(self.pid_file, "r") as f:
                pid = int(f.readline().rstrip())
            os.remove(self.pid_file)
            return pid
        except Exception as e:
            logger.log("ERROR", f"Error consuming PID file. Listener process at PID {pid} will not be terminated. See: {e}")
            return pid

    def stop(self):
        #stops listener executable
        pid = self._consume_pid_file()
        try:
            os.kill(pid, 9)
            logger.log("DEBUG", f"Terminated listener with PID {pid}")
        except Exception as e:
            logger.log("ERROR", f"Error terminating listener at PID {pid}. See: {e}")

    #cleanup is the same as stop
    def cleanup(self):
        self.stop()

def main():
    Kafka()

if __name__ == '__main__':
    main()