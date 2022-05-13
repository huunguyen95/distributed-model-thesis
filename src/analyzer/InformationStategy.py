# -*- coding: utf-8 -*-
from psutil import cpu_count, cpu_freq, cpu_percent, getloadavg
from psutil import virtual_memory
from time import sleep
from os import system, getenv
#from tcp_latency import measure_latency
from pythonping import ping
import urllib.request
import netifaces as ni
from socket import gethostname
from getmac import get_mac_address as gma
from kafka import KafkaProducer

import json
import platform
import logging
import coloredlogs

coloredlogs.install()
KAFKA_SERVER = '192.168.2.10'
KAFKA_PORT = '9092'
TOPIC = 'information-strategy-center-server'

'''
    collect resource information + status of the current node
'''


class InformationStrategy:
    def __init__(self, hostname=gethostname(),
                 src_server=None,
                 dst_server=KAFKA_SERVER,
                 dst_port=KAFKA_PORT,
                 topic=TOPIC,
                 cpu_threshold=90,
                 ram_threshold=90):

        super().__init__()
        logging.info(dst_server)
        logging.info(dst_port)
        self.kafka_ip = dst_server
        self.kafka_port = dst_port
        self.system_status = 'NOK'
        self.cpu_th = cpu_threshold
        self.ram_th = ram_threshold
        self.hostname = hostname
        self.mac_addr = None
        self.src_server_private_ip = src_server

        self.topic = topic

    #return: system status
    def get_system_status(self):
        return self.system_status

    #return: host name
    def get_hostname(self):
        self.hostname = gethostname()
        return self.hostname

    #return: list of all private IPs from all NICs
    def get_private_ip(self):
        private_nic_ip_addresses = []
        for each_ip in ni.interfaces():
            try:
                ip = ni.ifaddresses(each_ip)[ni.AF_INET][0]['addr']
                private_nic_ip_addresses.append(ip)
            except:
                pass
        self.src_server_private_ip = private_nic_ip_addresses[1]
        return self.src_server_private_ip

    #return: TCP ping value from src_host to dst_host
    def get_latency(self, ip="127.0.0.1"):
        return ping(ip, timeout=2, count=4).rtt_avg_ms

    #return mac_addr
    def get_mac_addr(self):
        self.mac_addr = gma()
        return self.mac_addr

    def get_mem_used(self):
        mem_used = virtual_memory()
        return mem_used[2]

    #publish resource information of current node to the broker
    def publish_node_info_to_broker(self, topic, data=None):
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_ip+':'+self.kafka_port)
        producer.send(topic, data)

    def do_monitor_system_resource_and_publish_to_kafka(self):
        system_metrics = {'mac_addr': self.get_mac_addr(), 'hostname': self.get_hostname(),
                          'src_server_private_ip': self.get_private_ip(), 'cpu_percent': cpu_percent(),
                          'mem_used': self.get_mem_used(), 'network_latency': self.get_latency(
                                                                                                ip=self.kafka_ip,
                                                                                                )}
        # only available RAM
        if system_metrics['cpu_percent'] < self.cpu_th or system_metrics['mem_used'] < self.ram_th:
            self.system_status = "OK"
        else:
            self.system_status = "NOK"
        system_metrics['system_status'] = self.system_status
            # PUBLISH TO BROKER for each 5 secs
        logging.info("publishing to kafka broker: ")
        logging.info(system_metrics)
        print("publishing to kafka broker: ", )
        self.publish_node_info_to_broker(
            topic=self.topic, data=json.dumps(system_metrics).encode('utf-8'))
            #sleep(0.05)
        #pass
