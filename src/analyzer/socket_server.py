import os
import shutil
import socket
import threading
import queue
import time

from psutil import cpu_percent, virtual_memory
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

transfer = TransferStrategy()
location = LocationStrategy()
info = InformationStrategy()

IP = "192.168.2.10"
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
q_tasks = queue.Queue()


def active_socket_server():
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")
        if q_tasks.qsize() != 0:
            handle_data(q_tasks.get())


def handle_client(conn, addr):
    while True:
        print(f"[NEW CONNECTION] {addr} connected.")
        conn.send("OK@Welcome to the File Server.".encode(format_encode))
        name = conn.recv(size).decode(format_encode)
        print(f"[RECEIVING] {name}")

        conn.send("[RECEIVED] FILE NAME".encode(format_encode))
        name = name.split("/")[-1]
        filepath = os.path.join(server_data_path, name)
        #os.system("rm -rf ./server_data/local/*")
        with open(filepath, "w") as f:
            while True:
                text = conn.recv(size).decode(format_encode)
                # print(text)
                f.write(text)
                if not text:
                    break
        f.close()
        # self.task_state = True
        ##add filename into queue
        q_tasks.put(filepath)
        print(f"[DISCONNECTED] {addr} disconnected")
        conn.close()
        #print(f"[TASK] {name}\n")


def handle_data(filepath):
    with open(filepath, "r") as f:
        data = f.read()
    f.close()
    src_ip = info.get_private_ip()
    name = filepath.split("/")[-1]
    key_info = src_ip+":"+name
    ##Check system state overload or not
    if cpu_percent() > 90 and virtual_memory()[2] > 90:
        logging.warning("=================SYSTEM OVERLOAD AND REQUEST SUPPORT==================")
        location.create_ksql_stream()
        location.query_candidates_with_low_latency()
        choosen_node = location.select_the_best()[1][0]
        transfer.kafka_publish_data(kafka_broker_ip=choosen_node,
                                    kafka_port=KAFKA_PORT,
                                    data_key=key_info,
                                    data_value=data)
        logging.info("HANDLING FILE %s with node: %s" % (q_tasks.get(), choosen_node))
    else:
        logging.info("=============SYSTEM UNDERLOAD AND HANDLE REQUEST BY ITS SELF============")
        os.system("rm -rf ./server_data/test_dataset/test_tasks/*")
        shutil.copyfile(filepath, f"./server_data/test_dataset/test_tasks/{name}")
        os.system("python3.8 ./Graph2vec.py")
        #time.sleep(5)
        os.system("python3.8 ./defensive_model.py")
        #time.sleep(5)
        logging.info("DONE TASK %s" % name)
        with open("./result/pred_result.txt", "r") as f:
            pred_result = f.read()
        f.close()
        transfer.kafka_publish_data(topic=OFFLOAD_RESULT_TOPIC, data_key=name, data_value=pred_result)


def main():
    active_socket_server()


if __name__ == "__main__":
    main()
