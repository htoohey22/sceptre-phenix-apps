import datetime
import os

from phenix_apps.apps.scorch import ComponentBase
from phenix_apps.common import logger, utils
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pathlib import Path

class Demo(ComponentBase):
    def __init__(self):
        ComponentBase.__init__(self, 'kafka')
        self.execute_stage()
    
    def start(self):
        logger.log('INFO', f'Starting user component: {self.name}')

    def stop(self):
        logger.log('INFO', f'Stopping user component: {self.name}')

def main():
    Demo()

if __name__ == '__main__':
    main()