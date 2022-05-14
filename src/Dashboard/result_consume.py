def kafka_consume_data(self, kafka_broker_ip="127.0.0.1", kafka_port="9092", topic="hand_shakes"):
    while True:
        kafka_consumer = KafkaConsumer(
            bootstrap_servers=kafka_broker_ip + ':' + kafka_port,
            request_timeout_ms=2000
        )
        logging.info("DONE setting up kafka consumer")
        kafka_consumer.subscribe(topic)
        for message in kafka_consumer:
        # print(message.key)
        # return message
        kafka_consumer.close()