import json, itertools, threading, time, sys, csv, re, os

from phenix_apps.apps.scorch import ComponentBase
from phenix_apps.common import logger, utils
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from datetime import datetime

run_loop = False

class kafka(ComponentBase):
    def __init__(self):
        ComponentBase.__init__(self, 'kafka')
        self.execute_stage()

    def start(self):
        global run_loop
        run_loop = True
        logger.log('INFO', f'Starting user component: {self.name}')

        #get all variables from tags
        bootstrapServers = self.metadata.get("bootstrapServers", ["172.20.0.63:9092"])
        topics = self.metadata.get("topics", None)
        csvBool = self.metadata.get("csv", True) #if false output a JSON

        #kafka consumer
        consumer = KafkaConsumer(
            #bootstrap ip and port could probably be separate variables in the future
            bootstrap_servers = bootstrapServers,
            auto_offset_reset='latest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        #list of all topic names we want the consumer to subscribe to
        subscribedTopics = []

        #get all topic names
        if not topics:
            logger.log('INFO', f'No topics subscribed to')
        else:
            for topic in topics:
                name =  topic.get("name")

                #handle wildcards in the name
                if '*' in name:
                    filteredName = name.split('*')[0]
                    pattern = f'^{re.escape(filteredName)}.*'
                    for topic in consumer.topics():
                        if str(filteredName) in str(topic):
                            subscribedTopics.append(topic)
                elif name:
                    subscribedTopics.append(name)

        logger.log('INFO', f'Subscribed Topics: {subscribedTopics}')
        #subscribe to all topic names
        consumer.subscribe(subscribedTopics)

        #get and output the output directory to the logger
        output_dir = self.base_dir
        logger.log('INFO', f'Output Directory: {output_dir}')
        os.makedirs(output_dir, exist_ok=True)

        all_keys = set()
        wrote_header = False

        try:
            #run the consumer, try to find all messages with the relevant tags
            if csvBool:
                with open(os.path.join(output_dir, 'out.csv'), mode="a", newline="", encoding="utf-8") as file:
                    writer = None
                    while run_loop:
                        for message in consumer:

                            #grab unfiltered/ unprocessed message data
                            data = message.value

                            #for each topic, check if this message has the desired key and value
                            for topic in topics:
                                for filterVal in topic.get("filter", []):
                                    key = filterVal.get("key")
                                    value = filterVal.get("value")

                                    if key in data:
                                        if str(data.get(key)).lower() == str(value).lower():
                                            all_keys.update(data.keys())

                                            if writer is None:
                                                writer = csv.DictWriter(file, fieldnames=sorted(all_keys), extrasaction='ignore')
                                                
                                                #check if the first line in the csv has been written yet, write it if not
                                                if not wrote_header:
                                                    writer.writeheader()
                                                    wrote_header = True

                                            storeMessage = True

                            if storeMessage:
                                #write the data and flush the data to ensure that we don't save to buffer
                                writer.writerow(data)
                                file.flush()

            else: #if not CSV, output JSON
                with open(os.path.join(output_dir, 'out.txt'), mode='a', encoding='utf-8') as file:
                    while run_loop:
                        for message in consumer:
                            #grab unfiltered/ unprocessed message data
                            data = message.value

                            storeMessage = False

                            #for each topic, check if this message has the desired key and value
                            for topic in topics:
                                for filterVal in topic.get("filter", []):
                                    key = filterVal.get("key")
                                    value = filterVal.get("value")

                                    if key in data:
                                        if str(data.get(key)).lower() == str(value).lower():
                                            storeMessage = True
                            
                            if storeMessage:
                                #write the data and flush the data to ensure that we don't save to buffer
                                file.write(json.dumps(data) + "\n")
                                file.flush()

        except Exception as e:
            logger.log('INFO', f'FAILED: {e}')
        finally:
            consumer.close()

        logger.log('INFO', f'Configured user component: {self.name}')

    def stop(self):
        logger.log('INFO', f'Stopping user component: {self.name}')
        global run_loop
        run_loop = False

    def cleanup(self):
        #no cleanup, currently it just makes and populates the one csv/json file
        logger.log('INFO', f'Cleaning up user component: {self.name}')

def main():
    kafka()
    
if __name__ == '__main__':
    main()