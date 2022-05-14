import logging
import os
import time
import sys
import socket
import threading
from InformationStategy import InformationStrategy
from socket_server import SocketServer
from LocationStategy import LocationStrategy
from TransferStategy import TransferStrategy
sys.path.extend(['/home/center/Master_prj'])


'''
This is model that control all the system at IoT Analyzer. Call the model and run code.
1. Check task assign
2. Check system state
3. Request offload
4. Transfer file
5. Receive predict result
6. Send to Dashboard
'''

##SYSTEM INFO###
TOPIC_INFORMATION_STRATEGY = "information-strategy-center-server"
CENTER_SERVER_CONFLUENT_CONN_STRING = "http://192.168.2.10:8088"
KAFKA_SERVER = '192.168.2.60'
KAFKA_PORT = '9092'
TOPIC_LOCATION_STRATEGY = 'location-strategy-center-server'
###TOPIC_NOTIFY_TRANSFER = 'hand_shakes' ###REEMOVE
TOPIC_OFF_LOAD_TASKS = 'off-load-tasks'
TOPIC_OFF_LOAD_result = 'off-load-results'
###SYSTEM FLAG###
###SYSTEM_STATUS = "OK"
###TASK_ASSIGN = True
###

####LISTEN FOLDER####
server_socket = SocketServer()
transfer = TransferStrategy()
location = LocationStrategy(center_server_conn_string=CENTER_SERVER_CONFLUENT_CONN_STRING,
                            kafka_server=KAFKA_SERVER,
                            kafka_port=KAFKA_PORT,
                            kafka_topic=TOPIC_LOCATION_STRATEGY)

####RUN ISOLATION
info = InformationStrategy(src_server=socket.gethostname(),
                           dst_server=KAFKA_SERVER,
                           dst_port=KAFKA_PORT,
                           topic=TOPIC_INFORMATION_STRATEGY)


def main():
    ###monitor resource of current server
    info.do_monitor_system_resource_and_publish_to_kafka()
    location.create_ksql_stream()
    server_socket.active_socket_server()
    while True:
        if server_socket.get_task_state():
            if info.get_system_status() == "NOK":
                logging.warning("SYSTEM OVERLOAD")
                ###Do off-load
                #Select the best node from LOCATION STRATEGY
                choosen_node = location.select_the_best(min_latency=10,
                                                     min_used_ram=90.0,
                                                     min_load_cpu=90.0,
                                                     mode="latency")
                ip_choosen_node = choosen_node[1][0]
                logging.info("CHOOSEN NODE: ")
                logging.info(choosen_node)
                transfer.kafka_publish_data(kafka_broker_ip=ip_choosen_node,
                                            kafka_port=9092,
                                            topic=TOPIC_NOTIFY_TRANSFER,
                                            data_key=ip_choosen_node,
                                            data_value=b"Hello! How are you?"
                                            )

            else:
                ###Do by its self
                logging.info("SYSTEM UNDERLOAD")
        transfer.kafka_consume_data(topic=TOPIC_NOTIFY_TRANSFER)


    #infor_thread = threading.Thread(target=run_period_info())
    #infor_thread.start()
    ##if there is a task assignment or system become overload
    #while True:
    #    if


##notify jobs
##choose target node
##hand shake
##send file =>source
##receive file => target
##handle job
##sendbackfile => target
##receive file => source


if __name__ == "__main__":
    main()
