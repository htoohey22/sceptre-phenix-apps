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

        #get directory, make output txt file
        output_dir = self.base_dir
        logger.log('INFO', f'Output directory: {output_dir}') 
        filePath = os.path.join(output_dir, f'i_wonder_what_the_time_is.txt')

        #check yaml to see if we want to include the year or not
        include_year = self.metadata.get("include_year", True)

        #write to the file
        f = open(filePath, "a")
        if include_year:
            f.write((datetime.datetime.now()).strftime("%m-%d %H:%M"))
        else:
            f.write(datetime.datetime.now())

    def stop(self):
        logger.log('INFO', f'Stopping user component: {self.name}')
        #nothing really needs to be stopped for this, we didn't start any services... but if we did, we could stop those here

def main():
    Demo()

if __name__ == '__main__':
    main()