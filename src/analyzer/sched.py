import logging
import os
import time
import sys
import socket
import threading
from InformationStategy import InformationStrategy
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
ip_center_server = "192.168.2.10"
port_center_server = "9092"
topic_information_stategy = "information-strategy-center-server"
###SYSTEM FLAG###
SYSTEM_STATUS = "OK"
task_assign = True

info = InformationStrategy(src_server=socket.gethostname(),
                           dst_server=ip_center_server,
                           dst_port=port_center_server,
                           topic=topic_information_stategy)

infor_strate_run_flag = True


def run_period_info():
    while True:
        info.do_monitor_system_resource_and_publish_to_kafka()
        time.sleep(5)
        if not infor_strate_run_flag:
            break


def monitor_system_status():
    while True:
        if info.get_system_status() == "NOK":
            logging.WARNING("SYSTEM OVERLOAD")
            '''
            TODO
            '''
    time.sleep(1)


def main():
    infor_thread = threading.Thread(target=run_period_info())
    infor_thread.start()
    monitor_thread = threading.Thread(target=monitor_system_status())
    monitor_thread.start()


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
