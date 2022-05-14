import queue
import threading

from kafka import KafkaProducer, KafkaConsumer
import logging
import coloredlogs
import os
from dotenv import load_dotenv
from socket import gethostname
import queue
import shutil
#from InformationStategy import InformationStrategy

coloredlogs.install()
load_dotenv(dotenv_path=".env")


'''
    Transfer strategy determine these things:
        - when to offloading
        - which job to offloading
        - how to offloading:
            - communication mechanism
            - 
    notify each other (via kafka) when transfering
'''
OFFLOAD_TASKS_TOPIC = os.environ.get("TOPIC_OFF_LOAD_TASKS")
OFFLOAD_RESULT_TOPIC = os.environ.get("TOPIC_OFF_LOAD_result")
KAFKA_PORT = os.environ.get("KAFKA_PORT")
DASHBOARD = os.environ.get("DASHBOARD")
DASHBOARD_PORT = os.environ.get("DASHBOARD_PORT")

LOCAL_IP = "127.0.0.1"
#REMOTE_IP = "192.168.1.60"
#KAFKA_PORT = "9092"


class TransferStrategy:
    def __init__(self):
        super().__init__()
        self.q_tasks = queue.Queue()

    def kafka_publish_data(self,
                           kafka_broker_ip="127.0.0.1",
                           kafka_port="9092",
                           topic="off-load-tasks",
                           data_key=None,
                           data_value=None,):
        kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker_ip+':'+kafka_port,
                                       max_request_size=20000000,
                                       compression_type='gzip')
        logging.info("DONE setting up Kafka Producer")
        #data_key = InformationStrategy.get_private_ip()
        #data_value = b"Hello! Are you free?"
        kafka_producer.send(topic, key=data_key.encode('utf-8'), value=data_value.encode('utf-8'))
        logging.info("SEND request to target node")
        kafka_producer.flush()
        kafka_producer.close()

    def kafka_consume_data(self, kafka_broker_ip="127.0.0.1", kafka_port="9092", topic=OFFLOAD_TASKS_TOPIC):
        while True:
            kafka_consumer = KafkaConsumer(
                bootstrap_servers=kafka_broker_ip+':'+kafka_port,
                request_timeout_ms=2000
            )
            logging.info("DONE setting up kafka consumer")
            kafka_consumer.subscribe(topic)
            for message in kafka_consumer:
                logging.info(message.key)
                filename = message.key
                filepath = os.path.join("./server_data/remote/", filename.spit(":")[-1])
                self.q_tasks.put(message.key)
                with open(filepath, "w") as f:
                    f.write(message.value)
                f.close()
            #return message
            kafka_consumer.close()


def main():
    #info1 = InformationStrategy()
    listen_results = TransferStrategy()
    monitor_tasks = threading.Thread(target=listen_results.kafka_consume_data())
    monitor_tasks.start()
    while True:
        if listen_results.q_tasks.qsize() != 0:
            task_info = listen_results.q_tasks.get()
            filename = task_info.split(":")[-1]
            src_ip = task_info.split(":")[0]
            filepath = os.path.join("./server_data/remote/", filename)
            with open(filepath, "r") as f:
                data = f.read()
            f.close()
            name = filepath.split("/")[-1]
            logging.info("SYSTEM UNDERLOAD AND HANDLE REQUEST BY ITS SELF")
            os.system("rm -rf ./server_data/test_dataset/test_tasks/*")
            shutil.copyfile(filepath, f"./server_data/test_dataset/test_tasks/{name}")
            os.system("python3.8 ./Graph2vec.py")
            os.system("python3.8 ./defensive_model.py")
            logging.info("DONE TASK %s" % name)
            with open("./result/pred_result.txt", "r") as f:
                pred_result = f.read()
            f.close()
            listen_results.kafka_publish_data(kafka_broker_ip=src_ip,
                                              kafka_port=KAFKA_PORT,
                                              topic=OFFLOAD_RESULT_TOPIC,
                                              data_key=name,
                                              data_value=pred_result)


if __name__ == "__main__":
    main()
