import datetime
import sys
import os
import time

from phenix_apps.apps.scorch import ComponentBase
from phenix_apps.common import logger, utils
from pathlib import Path

class Demo(ComponentBase):
    def __init__(self):
        ComponentBase.__init__(self, 'Demo')
        self.execute_stage()
    
    def start(self):
        logger.log('INFO', f'Starting user component: {self.name}')

        #get directory, make output txt file
        output_dir = self.base_dir
        logger.log('INFO', f'Output directory: {output_dir}') 
        filePath = os.path.join(output_dir, f'what_time_is_it.txt')
        f = open(filePath, "a")

        #check yaml to see if we want to include the year or not
        include_year = self.metadata.get("include_year", True)

        f.write(str(1.0))

        if include_year:
            f.write(str(datetime.datetime.now()))
        else:
            f.write(str((datetime.datetime.now()).strftime("%m-%d %H:%M")))

    def stop(self):
        #nothing really needs to be stopped for this, we didn't start any services... but if we did, we could stop those here
        logger.log('INFO', f'Stopping user component: {self.name}')

def main():
    Demo()

if __name__ == '__main__':
    main()