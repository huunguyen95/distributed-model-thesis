from kafka import KafkaConsumer


def kafka_consume_data(kafka_broker_ip="127.0.0.1", kafka_port="9092", topic="hand_shakes"):
    while True:
        kafka_consumer = KafkaConsumer(
            bootstrap_servers=kafka_broker_ip + ':' + kafka_port,
            request_timeout_ms=2000
        )
        #logging.info("DONE setting up kafka consumer")
        kafka_consumer.subscribe(topic)
        for message in kafka_consumer:
            print(message)
        # return message
        kafka_consumer.close()


kafka_consume_data("10.88.0.79", "9092", "data_process_app-predict_table-changelog")
#kafka_consume_data("10.88.0.41", "9092", "data_process_app-predict_table-changelog")
