import time
import random
import logging
import ssl
import os
import socket
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from message import Message, MessageSerializer

INSTANCE_ID = os.getenv('HOSTNAME', socket.gethostname())
base_path = "/app/kafka-client"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('producer')

def produce_messages():
    import uuid
    try:
        producer_conf = {
            "bootstrap_servers": "kafka-1:9093,kafka-2:9093,kafka-3:9093",
            "security_protocol": "SSL",
            "ssl_check_hostname": False,
            "ssl_cafile": f"{base_path}/ca.crt",
            "ssl_certfile": f"{base_path}/client.crt",
            "ssl_keyfile": f"{base_path}/client.key",
            "value_serializer": lambda v: str(v).encode('utf-8'),
            "key_serializer": lambda k: str(k).encode('utf-8'),
        }
        producer = KafkaProducer(**producer_conf)

        while True:
            key = f"key-{uuid.uuid4()}"
            value = f"SSL test message at {datetime.now()}"
            try:
                future = producer.send("topic-1", key=key, value=value)
                future2 = producer.send("topic-2", key=key, value=value)
                record_metadata = future.get(timeout=10)
                print(f"========== SSL message sent successfully!")
                print(f"Topic: {record_metadata.topic}")
                print(f"Partition: {record_metadata.partition}")
                print(f"Offset: {record_metadata.offset}")
                print(f"Key: {key}")
                print(f"Value: {value}")
            except Exception as e:
                print(f"Ошибка отправки сообщения: {e}")

            time.sleep(3)

    except Exception as e:
        print(f"Ошибка соединения: {e}")

if __name__ == "__main__":
    produce_messages()