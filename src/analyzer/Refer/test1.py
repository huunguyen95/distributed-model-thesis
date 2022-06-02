from confluent_kafka import Producer
import json
import time
import random
import uuid

new_message = {"IP": "192.168.2.9", "timestamp": time.time(), "filename": "0a1830c9206118843062d06d668c89622b88bea97fdb189e80b060d50905e7e3.adjlist", "filepath": "./server_data/local/0a1830c9206118843062d06d668c89622b88bea97fdb189e80b060d50905e7e3.adjlist"}
new_message1 = {"IP": "192.168.2.9", "timestamp": time.time(), "filename": "0c3583b5cd35e822cc0d9f2fca77c3ee7fa7208e7028d3f38cc623f51e165218.adjlist", "filepath": "./server_data/local/0c3583b5cd35e822cc0d9f2fca77c3ee7fa7208e7028d3f38cc623f51e165218.adjlist"}


def kafka_producer():
    bootstrap_servers = "192.168.2.10:9092"
    topic = "local_task"
    p = Producer({'bootstrap.servers': bootstrap_servers})
    total = 1
    count = 0
    #while total:
    for i in range(2):
        #count, base_message = generate_random_time_series_data(count)
        #total -= 1

        record_key = "192.168.2.9"
        if i == 0:
            record_value = json.dumps(new_message)
        else:
            record_value = json.dumps(new_message1)

        p.produce(topic, key=record_key, value=record_value)
        p.poll(0)

    p.flush()
    #print('we\'ve sent {count} messages to {brokers}'.format(count=count, brokers=bootstrap_servers))


if __name__ == "__main__":
    kafka_producer()
