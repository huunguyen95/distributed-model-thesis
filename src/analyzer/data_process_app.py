import faust
import os
from dotenv import load_dotenv
import logging
import coloredlogs
from psutil import cpu_percent, virtual_memory
import shutil
from LocationStategy import LocationStrategy
from confluent_kafka import Producer
import json
import time
from socket import gethostname

##LOGGING
coloredlogs.install()
load_dotenv(dotenv_path=".env")

###ENV
IP = os.environ.get(gethostname())
local_task_topic = os.environ.get("TOPIC_LOCAL_TASK")
handle_data_topic = os.environ.get("TOPIC_HANDLE_DATA")
OFFLOAD_RESULT_TOPIC = os.environ.get("TOPIC_OFF_LOAD_result")
location = LocationStrategy()
bootstrap_server = IP + ":9092"
local_broker = "kafka://"+bootstrap_server
center_bootstrap = os.environ.get("KAFKA_SERVER")

##RUN the FAUST APP
app = faust.App('data_process_app', broker=local_broker)


class data(faust.Record, validation=True):
    IP: str
    timestamp: float
    filename: str
    filepath: str


def kafka_producer(bootstrap_servers=bootstrap_server, topic=local_task_topic, record_key=IP, new_message=None):
    #bootstrap_servers = "192.168.2.10:9092"
    #topic = "local_task"
    p = Producer({'bootstrap.servers': bootstrap_servers})
    #record_key = "192.168.2.9"
    record_value = json.dumps(new_message)
    p.produce(topic, key=record_key, value=record_value)
    p.poll(0)
    p.flush()


local_task = app.topic(local_task_topic, value_type=data)
handle_data = app.topic(handle_data_topic, internal=True, partitions=1, value_type=data)
predict_table = app.Table("predict_table", key_type=str, value_type=str, partitions=1, default=int)


@app.agent(local_task)
async def process_data(data_):
    async for count in data_:
        print(f"Data recieved is {count}")
        ##Check system state overload or not
        if cpu_percent() > 90.0 or virtual_memory()[2] > 90.0:
            logging.warning("=================SYSTEM OVERLOAD AND REQUEST SUPPORT==================")
            choosen_node = location.select_the_best()[1][0]
            ##hard coding ==> can be changed in any system
            os.system(f"scp -i ./ssh_key/huytrung -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null {count.filepath} huytrung@{choosen_node}:/data/analyzer/server_data/remote/{count.filename}")
            info = {"IP": IP, "timestamp": time.time(), "filename": count.filename, "filepath": f"./server_data/remote/{count.filename}"}
            kafka_producer(bootstrap_servers=choosen_node+":9092", topic=local_task_topic, record_key=IP, new_message=info)
        else:
            logging.warning("=============SYSTEM UNDERLOAD AND HANDLE REQUEST BY ITS SELF============")
            await handle_data.send(value=count)


@app.agent(handle_data)
async def predic_model(counts):
    async for count in counts:
        print(f"Handle file {count}")
        #os.system("rm -rf ./server_data/test_dataset/test_tasks/*")
        #shutil.copyfile(count.filepath, f"./server_data/test_dataset/test_tasks/{count.filename}")
        #os.system("python3 ./Graph2vec.py")
        prefix = "python3 ./predict.py " + str(count.filepath)
        os.system(prefix)
        with open("./result/pred_result.json", "r") as f:
            pred_result = json.load(f)
        f.close()
        predict_table[str(count.filename)] = pred_result
        kafka_producer(count.IP+":9092", "predict_result", IP, pred_result)
        kafka_producer(center_bootstrap + ":9092", "predict_result", IP, pred_result)
        #print(f'{str(count.userId)} has now been seen {count_table[str(count.userId)]} times')
