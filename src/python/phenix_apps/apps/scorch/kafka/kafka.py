import json, itertools, threading, time, sys, csv, re, os

from phenix_apps.apps.scorch import ComponentBase
from phenix_apps.common import logger, utils
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from datetime import datetime

scorch_kafka_running = False

class kafka(ComponentBase):
    def __init__(self):
        ComponentBase.__init__(self, 'kafka')
        self.execute_stage()

    def start(self):
        global scorch_kafka_running
        scorch_kafka_running = True
        logger.log('INFO', f'Starting user component: {self.name}')

        #get all variables from tags
        kafka_ips = self.metadata.get("kafka_ips", ["172.20.0.63:9092"])
        topics = self.metadata.get("topics", None)
        csvBool = self.metadata.get("csv", True) #if false output a JSON

        #kafka consumer
        consumer = KafkaConsumer(
            #bootstrap ip and port could probably be separate variables in the future
            bootstrap_servers = kafka_ips,
            auto_offset_reset='latest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        #list of all topic names we want the consumer to subscribe to
        subscribedTopics = []
        foundTopics = False

        #get all topic names
        if not topics:
            logger.log('INFO', f'No topics subscribed to')
        
        for topic in topics:
            name =  topic.get("name")

            #handle wildcards in the name, this only supports right wildcards
            if '*' in name:
                foundTopics = False
                filteredName = name.split('*')[0] #we don't care about anything right of the wildcard
                pattern = f'^{re.escape(filteredName)}.*'
                while foundTopics == False: #if this is a new experiment, kafka may not have populated any tags... so wait until it has
                    for topic in consumer.topics():
                        if str(filteredName) in str(topic):
                            subscribedTopics.append(topic)
                    if subscribedTopics:
                        foundTopics = True
                    else:
                        time.sleep(5) #we don't need to be constantly scaning for data, so sleep for a few seconds imbetween attempts
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

        def helper(csvBool, path):
            try:
                with open(path, 'a', newline='', encoding='utf-8') as file:
                    writer = None
                    wrote_header = False
                    all_keys = set()

                    while scorch_kafka_running:
                        for message in consumer:
                            storeMessage = False

                            #grab unfiltered/ unprocessed message data
                            data = message.value

                            #for each topic, check if this message has the desired key and value
                            for topic in topics:
                                for filterVal in topic.get("filter", []):
                                    key = filterVal.get("key")
                                    value = filterVal.get("value")

                                    wildcardValue = False

                                    if key in data:
                                        actualValue = str(data.get(key)).lower()
                                        pattern = str(value).lower()
                                        
                                        #use regular expressions to account for wildcards
                                        pattern = re.escape(pattern).replace(r'\*', '.*')

                                        regex = re.compile(f"^{pattern}$", re.IGNORECASE)
                                        if regex.match(actualValue):
                                            if csvBool:
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
                                if csvBool:
                                    writer.writerow(data)
                                else:
                                    file.write(json.dumps(data) + "\n")
                                file.flush()
            except Exception as e:
                logger.log('INFO', f'THREAD FAILED: {e}')
            finally:
                logger.log('INFO', 'EXITING THREAD.')
                #consumer.close()
                #scorch_kafka_running = False

        try:
            #run the consumer, try to find all messages with the relevant tags
            if csvBool:
                path = os.path.join(output_dir, 'out.csv')
            else:
                path = os.path.join(output_dir, 'out.txt')
            
            t1 = threading.Thread(target=helper, args=(csvBool,))
            t1.start()
        except Exception as e:
            logger.log('INFO', f'FAILED: {e}')
        finally:
            logger.log('INFO', 'CLOSING.')

    def stop(self):
        logger.log('INFO', f'Stopping user component: {self.name}')
        #global scorch_kafka_running
        #scorch_kafka_running = False
        #t1.join()

    def cleanup(self):
        #no cleanup, currently it just makes and populates the one csv/json file
        logger.log('INFO', f'Cleaning up user component: {self.name}')
        #global scorch_kafka_running
        #scorch_kafka_running = False
        #t1.join()

def main():
    kafka()
    
if __name__ == '__main__':
    main()