import json, itertools, threading, time, sys, csv, re

from phenix_apps.apps.scorch import ComponentBase
from phenix_apps.common import logger, utils
from datetime import datetime

class kafka(ComponentBase):
    def __init__(self):
        ComponentBase.__init__(self, 'kafka')
        self.execute_stage()

    def start(self):
        logger.log('INFO', f'Starting user component: {self.name}')

        logger.log('INFO', f'Configured user component: {self.name}')

    def stop(self):
        #it should run as long as the experiment runs, so I don't think anything needs to be here
        logger.log('INFO', f'Stopping user component: {self.name}')

    def cleanup(self):
        #no cleanup, currently it just makes and populates the one csv file
        logger.log('INFO', f'Cleaning up user component: {self.name}')

def main():
    kafka()
    
if __name__ == '__main__':
    main()