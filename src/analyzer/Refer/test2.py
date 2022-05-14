from ksql import KSQLAPI
import logging
import coloredlogs
import json
import socket
import threading
from kafka import KafkaProducer, KafkaConsumer
from InformationStategy import InformationStrategy
import os
import subprocess

coloredlogs.install()
os.system("python3.8 /home/center/Master_prj/distributed-model-thesis/src/analyzer/test.py")
print(10)
#while os.system("python3.8 /home/center/Master_prj/distributed-model-thesis/src/analyzer/test.py"):
#    pass

#subprocess.call(('someprog.exe', str(i)))
"""
def kafka_pubnish_data(kafka_broker_ip="127.0.0.1",
                       kafka_port="9092",
                       topic="off-load-tasks",
                       data_key=None,
                       data_value=b"Hello! Are you free?"):
    kafka_producer = KafkaProducer(
        bootstrap_servers=kafka_broker_ip + ':' + kafka_port,
        max_request_size=50000000,
        compression_type='gzip'
        )
    logging.info("DONE setting up Kafka Producer")
        #data_key = InformationStrategy.get_private_ip()
        #data_value = b"Hello! Are you free?"
    kafka_producer.send(topic, key=data_key, value=data_value)
    logging.info("SEND request to target node")
    kafka_producer.close()


def kafka_consumer_(kafka_broker_ip="127.0.0.1", kafka_port="9092"):
    kafka_consumer = KafkaConsumer(
        bootstrap_servers=kafka_broker_ip + ':' + kafka_port,
        request_timeout_ms=2000
    )
    logging.info("DONE setting up kafka consumer")
    kafka_consumer.subscribe('off-load-tasks')
    for message in kafka_consumer:
        #print(message.value)
        return message
    kafka_consumer.close()


def monitor_consumer():
    #while True:
    print(kafka_consumer_())


def main():
    newthread = threading.Thread(target=monitor_consumer())
    newthread.start()
    #path = "./server_data/malware/176fc651843f3391f1d21ba9b4a77d77ce5eaeb72f8a4275e3d1f3048a19765b.adjlist"
    #with open(path, "r") as f:
    #    data = f.read()
    #f.close()
    #a = path.split("/")[-1]
    #logging.info(a)
    #logging.info(data)
    #kafka_pubnish_data(data_key=a.encode('utf-8'), data_value=data.encode('utf-8'))


if __name__ == "__main__":
    main()


"""