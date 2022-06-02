import os
import shutil
import socket
import threading
import time
from confluent_kafka import Producer
import json
import time
import logging
import coloredlogs
from TransferStategy import TransferStrategy
from LocationStategy import LocationStrategy
from InformationStategy import InformationStrategy
from dotenv import load_dotenv


coloredlogs.install()
load_dotenv(dotenv_path=".env")
'''
This module have responsibility:
1. Receive raw data from IoT Agent
2. Send result to dashboard
'''

OFFLOAD_TASKS_TOPIC = os.environ.get("TOPIC_OFF_LOAD_TASKS")
OFFLOAD_RESULT_TOPIC = os.environ.get("TOPIC_OFF_LOAD_result")
KAFKA_PORT = os.environ.get("KAFKA_PORT")
DASHBOARD = os.environ.get("DASHBOARD")
DASHBOARD_PORT = os.environ.get("DASHBOARD_PORT")
LOCAL_TASK = os.environ.get("TOPIC_LOCAL_TASK")

transfer = TransferStrategy()
location = LocationStrategy()
location.create_ksql_stream()
info = InformationStrategy()

IP = os.environ.get(socket.gethostname())
PORT = 4456
ADDR = (IP, PORT)
size = 4096
format_encode = "utf-8"
server_data_path = "server_data/local"
print("[STARTING] Server is starting")
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)
server.listen()
print(f"[LISTENING] Server is listening on {IP}:{PORT}.")
local_bootstrap_server = IP+":9092"


def kafka_producer(bootstrap_servers=local_bootstrap_server, topic=LOCAL_TASK, record_key=IP, new_message=None):
    #bootstrap_servers = "192.168.2.10:9092"
    #topic = "local_task"
    p = Producer({'bootstrap.servers': bootstrap_servers})
    #record_key = "192.168.2.9"
    record_value = json.dumps(new_message)
    p.produce(topic, key=record_key, value=record_value)
    p.poll(0)
    p.flush()


def active_socket_server():
    location.create_ksql_stream()
    # executor = ThreadPoolExecutor(1)
    while True:
        conn, addr = server.accept()
        # thread = threading.Thread(target=handle_client, args=(conn, addr))
        # thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")
        conn.send("OK@Welcome to the File Server.".encode(format_encode))
        name = conn.recv(size).decode(format_encode)
        print(f"[RECEIVING] {name}")

        conn.send("[RECEIVED] FILE NAME".encode(format_encode))
        name = name.split("/")[-1]
        filepath = os.path.join(server_data_path, name)
        # os.system("rm -rf ./server_data/local/*")
        with open(filepath, "w") as f:
            while True:
                text = conn.recv(size).decode(format_encode)
                # print(text)
                f.write(text)
                if not text:
                    break
        f.close()
        ##########=============Producer=============
        new_message = {"IP": IP, "timestamp": time.time(),
                       "filename": name,
                       "filepath": filepath}
        kafka_producer(local_bootstrap_server, LOCAL_TASK, IP, new_message)
        ############################
        logging.info(filepath)
        print(f"[DISCONNECTED] {addr} disconnected")
        conn.close()
        logging.warning(f"===========================================1[STARTING TIMER][TASK] {name}\n")


def main():
    active_socket_server()


if __name__ == "__main__":
    main()
