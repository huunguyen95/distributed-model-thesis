from ksql import KSQLAPI
import logging
import coloredlogs
import json
import socket
coloredlogs.install()

client = KSQLAPI('http://localhost:8088')
stream = client.ksql('''drop stream if exists information_strategy_center_server;
                    create stream if not exists information_strategy_center_server (
                            src_server_private_ip varchar,
                            mac_addr varchar,
                            hostname varchar,
                            cpu_percent double,
                            mem_used int,
                            network_latency double,
                            system_status varchar)
                        with (kafka_topic='information-strategy-center-server',format='json')
                    ''')

query_for_is = "select * from information_strategy_center_server emit changes limit " + \
               str(5)
query = client.query(query_for_is)

for records in query:
    logging.info(records)
