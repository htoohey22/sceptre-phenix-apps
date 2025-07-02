#this file is run on a separate pid so that the component can run in the background without holding up other scorch components (scorch doesn't allow threads to run detatched)

import json
import itertools
import threading
import time
import sys
import csv
import re
import os
from phenix_apps.apps.scorch import ComponentBase
from phenix_apps.common import logger, utils
import kafka

def run(csvBool, path, kafka_ips, topics):
    kafka_ips = kafka_ips.split(',')
    topics = json.loads(topics)

    #kafka consumer
    consumer = KafkaConsumer(
        #bootstrap ip and port could probably be separate variables
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
        logger.log('ERROR', 'No topics subscribed to')
        exit() #TODO: make it subscribe to all topics when none are selected

    for topic in topics:
        name =  topic.get("name")

        #handle wildcards in the name, this only supports right wildcards
        if '*' in name:
            foundTopics = False
            filteredName = name.split('*')[0] #we don't care about anything right of the wildcard
            pattern = f'^{re.escape(filteredName)}.*'
            while not foundTopics: #if this is a new experiment, kafka may not have populated any tags... so wait until it has
                for topic in consumer.topics():
                    if str(filteredName) in str(topic):
                        subscribedTopics.append(topic)
                if subscribedTopics:
                    foundTopics = True
                else:
                    time.sleep(5) #we don't need to be constantly scaning for data, so sleep for a few seconds imbetween attempts
        elif name:
            subscribedTopics.append(name)
    with open(path, 'a', newline='', encoding='utf-8') as file:
        writer = None
        wrote_header = False
        all_keys = set()

        #subscribe to all topic names
        consumer.subscribe(subscribedTopics)

        while True:
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
                    logger.log('INFO', f'MESSAGE: {data}')
                    #write the data and flush the data to ensure that we don't save to buffer
                    if csvBool:
                        writer.writerow(data)
                    else:
                        file.write(json.dumps(data) + "\n")
                    file.flush()


def main():
    run()

if __name__ == '__main__':
    logger.log('INFO', 'HERE')
    #unpack the args
    csvBool = sys.argv[1].lower() == 'true'
    path = sys.argv[2]
    kafka_ips = sys.argv[3]
    topics = sys.argv[4]

    run(csvBool, path, kafka_ips, topics)