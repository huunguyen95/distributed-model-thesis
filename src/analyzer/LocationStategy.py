
from ksql import KSQLAPI
from ksql.errors import KSQLError
from contextlib import suppress
from operator import itemgetter
from kafka import KafkaProducer
from time import sleep
from os import getenv
from tcp_latency import measure_latency
from random import randint
from pythonping import ping

import logging
import coloredlogs
import json
import socket
coloredlogs.install()


# CONSTANTs
CENTER_SERVER_CONFLUENT_CONN_STRING = "http://192.168.2.10:8088"
##
KAFKA_SERVER = '192.168.1.60'
KAFKA_PORT = '9092'
TOPIC = 'location-strategy-center-server'
##
'''
    select the available node to offload:
        - get list of current available nodes via Kafka
        - select
        - publish some info to kafka

'''


class LocationStrategy:
    def __init__(self,
                 center_server_conn_string=CENTER_SERVER_CONFLUENT_CONN_STRING,
                 kafka_server=KAFKA_SERVER,
                 kafka_port=KAFKA_PORT,
                 kafka_topic=TOPIC):
        super().__init__()

        self.kafkaProducer = KafkaProducer(
            bootstrap_servers=kafka_server + ":" + kafka_port
        )
        self.ksqlClient = KSQLAPI(center_server_conn_string)
        logging.basicConfig(level=logging.INFO)
        self.candidates = {}
        self.lowestLatencyCandidates = []
        self.topic = kafka_topic
    '''
        at the very first level, just use a predefined list of possible candidates
    '''

    def create_ksql_stream(self):
        try:
            create_stream = self.ksqlClient.ksql('''
                                        drop stream if exists information_strategy_center_server;
                                        create stream if not exists information_strategy_center_server (
                                            src_server_private_ip varchar,
                                            mac_addr varchar,
                                            hostname varchar,
                                            cpu_percent double,
                                            mem_used int,
                                            network_latency double,
                                            system_status varchar
                                            )
                                        with (kafka_topic='information-strategy-center-server',format='json'    )
                                        ''')
        except KSQLError as e:
            logging.error("sSTH went wrong with KSQL : %r", e)
            return -1
        pass

    '''
        query the candidate which has LOWEST latency DIRECTLY from the KSQL stream on Center server
        save the list on self.lowestLatencyCandidates
        @return [
            {private_ip,mac_addr,hostname,cpu_avgload,mem_free,network_latency,public_ip}
            ]
        TODO: query for the number of nodes that currently publishing to information strategy topic then adjust the emitting changes in the stream
    '''

    def query_candidates_with_low_latency(self, preferred_latency=10, num_of_publishing_nodes=5):
        # reset candidates list
        self.lowestLatencyCandidates = []
        self.candidates = []
        query_for_is = "select * from information_strategy_center_server emit changes limit " + \
            str(num_of_publishing_nodes)
        query = self.ksqlClient.query(query_for_is)

        already_added_candidates = []
        # need better try-catch logic....
        # with suppress(RuntimeError):
        try:
            for records in query:
                # logging.info(records)
                if "row" in records:
                    # print("processing : ", records[0:-2])
                    r = json.loads(records[0:-2])

                    private_ip = r['row']['columns'][0]
                    mac_addr = r['row']['columns'][1]
                    hostname = r['row']['columns'][2]
                    cpu_avg_load = r['row']['columns'][3]
                    ram_cap = r['row']['columns'][4]
                    net_latency = r['row']['columns'][5]
                    system_state = r['row']['columns'][6]

                    if system_state == "OK":
                        self.candidates.append([hostname, (
                                private_ip,  # 0
                                mac_addr,  # 1
                                cpu_avg_load,  # 2
                                ram_cap,  # 3
                                net_latency,  # 4,
                                system_state #5
                            )])
        except RuntimeError:
            logging.error("done querying stream KSQL")
            pass
            #     logging.error("KSQLDB timed out...... SADLY")
            #     self.candidates = []
        except socket.timeout:
            logging.error("socket error")

        for each in self.candidates:
            try:
                # logging.info(each[1][0])
                current_latencee = ping(str(each[1][0]), timeout=2, count=4).rtt_avg_ms #currently  use private IP
                logging.info("latency from  here to -> %r: %r" %
                             (each[1][0], current_latencee))
                # logging.info(each[0])
                # logging.info(min_latency)

                if current_latencee <= preferred_latency and each[0] not in already_added_candidates:
                    self.lowestLatencyCandidates.append(each)
                    already_added_candidates.append(each[0])
                    # logging.info("possible candidates with low network latency: %r " % (
                    #     [x for x in self.lowestLatencyCandidates]))
            except TypeError as e:
                logging.error("having trouble with pinging command")
                logging.error(e)
        return self.lowestLatencyCandidates

    def select_the_best(self, min_latency=10, min_used_ram=90.0, min_load_cpu=90.0, mode="latency"):
        potential_latency_node = []
        potential_ram_node = []
        potential_cpu_node = []
        for each_candidates in self.query_candidates_with_low_latency(preferred_latency=10, num_of_publishing_nodes=5):
            if each_candidates[1][4] <= min_latency:
                potential_latency_node = each_candidates
            if each_candidates[1][3] <= min_used_ram:
                potential_ram_node = each_candidates
            if each_candidates[1][2] <= min_load_cpu:
                potential_cpu_node = each_candidates
        if mode == "latency":
            data = json.dumps(potential_latency_node).encode('utf-8')
            key = json.dumps("latency").encode('utf-8')
            self.kafkaProducer.send(self.topic, key=key, value=data)
            self.kafkaProducer.flush()
            self.kafkaProducer.close()
            logging.info(f"BEST LATENCY NODE: {potential_latency_node[0]}")
            return potential_latency_node
        elif mode == "cpu":
            data = json.dumps(potential_cpu_node).encode('utf-8')
            key = json.dumps("cpu_avgload").encode('utf-8')
            self.kafkaProducer.send(self.topic, key=key, value=data)
            self.kafkaProducer.flush()
            self.kafkaProducer.close()
            logging.info(f"BEST CPU NODE: {potential_cpu_node[0]}")
            return potential_cpu_node
        else:
            data = json.dumps(potential_ram_node).encode('utf-8')
            key = json.dumps("ram_cap").encode('utf-8')
            self.kafkaProducer.send(self.topic, key=key, value=data)
            self.kafkaProducer.flush()
            self.kafkaProducer.close()
            logging.info(f"BEST RAM NODE: {potential_ram_node[0]}")
            return potential_ram_node


    '''
        notify each others about possible candidates according to specified criteria
    '''

'''
def main():

    logging.info(CENTER_SERVER_CONFLUENT_CONN_STRING)
    locationStrategy = LocationStrategy(CENTER_SERVER_CONFLUENT_CONN_STRING)
    logging.info(locationStrategy)
    locationStrategy.create_ksql_stream()
    # locationStrategy.get_all_possible_candidates()

    while True:
        locationStrategy.query_candidates_with_low_latency()
        locationStrategy.select_candidates_by_ram_cap()
        locationStrategy.select_candidates_by_cpu_avg()
        # logging.info("wow i  found the most compatible node with lowest latency\n... %r"%(locationStrategy.select_the_lowest_latency()))
        # logging.info("otherwise we have the RAM maniac: %r"%(locationStrategy.select_candidates_by_ram_cap()))
        # logging.info("otherwise we have the CPU maniac: %r"%(locationStrategy.select_candidates_by_cpu_avg()))
        # print("final candidates list: ",locationStrategy.lowestLatencyCandidates)
        # locationStrategy.publish_lowest_latency_candidates_to_kafka()
        # locationStrategy.publish_lowest_CPU_avgload_candidates_to_kafka()
        # locationStrategy.publish_highest_RAM_cap_candidates_to_kafka()
        # print("done publish to kafka...")

        the_chosen_one = locationStrategy.select_candidates_to_offload(selection_criteria="latency")
        # print(the_chosen_one)
        logging.info("I am the chosen one %r" % the_chosen_one)
        # sleep(1)
        # print(chr(27)+'[2j')
        # print('\033c')
        # print('\x1bc')
        # sleep(0.25)


if __name__ == "__main__":
    main()
'''