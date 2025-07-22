import datetime
import os
import json
import itertools
import time
import sys
import csv
import re
import os
import time

from phenix_apps.apps.scorch import ComponentBase
from phenix_apps.common import logger, utils

from phenix_apps.apps.scorch import ComponentBase
from phenix_apps.common import logger, utils
from pathlib import Path

class Demo(ComponentBase):
    def __init__(self):
        ComponentBase.__init__(self, 'kafka')
        self.execute_stage()
    
    def start(self):
        logger.log('INFO', f'Starting user component: {self.name}')

        #get directory, make output txt file
        output_dir = self.base_dir
        logger.log('INFO', f'Output directory: {output_dir}') 
        filePath = os.path.join(output_dir, f'i_wonder_what_the_time_is.txt')

        #write to the file
        f = open(filePath, "a")
        f.write(str(datetime.datetime.now()))

        #check yaml to see if we want to include the year or not
        include_year = self.metadata.get("include_year", True)

        #if include_year:
        #    f.write((datetime.datetime.now()).strftime("%m-%d %H:%M"))
        #else:
        #    f.write(datetime.datetime.now())

    def stop(self):
        logger.log('INFO', f'Stopping user component: {self.name}')
        #nothing really needs to be stopped for this, we didn't start any services... but if we did, we could stop those here

def main():
    Demo()

if __name__ == '__main__':
    main()