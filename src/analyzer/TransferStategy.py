from kafka import KafkaProducer, KafkaConsumer
import logging
import coloredlogs
coloredlogs.install()
# from InformationStrategy import InformationStrategy

'''
    Transfer strategy determine these things:
        - when to offloading
        - which job to offloading
        - how to offloading:
            - communication mechanism
            - 
    notify each other (via kafka) when transfering
'''
OFFLOAD_TASKS_TOPIC = "off-load-tasks"
OFFLOAD_RESULT_TOPIC = "off-load-results"
HANDSHAKES_TOPIC = "hand_shakes"

LOCAL_IP = "192.168.1.49"
REMOTE_IP = "192.168.1.60"
KAFKA_PORT = "9092"


class TransferStrategy:
    def __init__(self):
        super().__init__()

    def kafka_pubnish_data(self,
                           kafka_broker_ip="127.0.0.1",
                           kafka_port="9092",
                           topic="hand_shakes",
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

    def kafka_consume_data(self, kafka_broker_ip="127.0.0.1", kafka_port="9092", topic="hand_shakes"):
        kafka_consumer = KafkaConsumer(
            bootstrap_servers=kafka_broker_ip+':'+kafka_port,
            request_timeout_ms=2000
        )
        logging.info("DONE setting up kafka consumer")
        kafka_consumer.subscribe(topic)
        for message in kafka_consumer:
            #print(message.key)
            return message
        kafka_consumer.close()

