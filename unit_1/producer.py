import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from message import Message, MessageSerializer
import os
import socket

# получаем идентификатор инстанса
INSTANCE_ID = os.getenv('HOSTNAME', socket.gethostname())

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('producer')

class MessageProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.instance_id = INSTANCE_ID
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            # Гарантия At Least Once
            acks='all',  # Ждем подтверждения от всех реплик
            retries=5,   # Количество повторных попыток
            retry_backoff_ms=1000,  # Задержка между повторными попытками
            batch_size=16384,  # Размер батча в байтах
            linger_ms=10,  # Максимальное время ожидания заполнения батча
            buffer_memory=33554432,  # Размер буфера в байтах
            # Сериализаторы
            key_serializer=str.encode,
            value_serializer=MessageSerializer.serialize_json,
            max_in_flight_requests_per_connection=1,  # Гарантия порядка сообщений
            request_timeout_ms=30000,  # Таймаут запроса
            # параметры для надежности
            max_block_ms=60000,  # Максимальное время блокировки
            metadata_max_age_ms=300000,  # Обновление метаданных каждые 5 минут
        )
        logger.info(f"Producer initialized for topic: {topic}")

    def send_message(self, message: Message) -> bool:
        """Отправка сообщения с гарантией доставки"""
        try:
            # Вывод сообщения в консоль перед отправкой
            print(f"[{self.instance_id}] Sending message: {message}")

            # Асинхронная отправка
            future = self.producer.send(
                topic=self.topic,
                key=str(message.id),
                value=message
            )

            # Синхронное ожидание подтверждения
            record_metadata = future.get(timeout=10)

            logger.info(f"Message delivered to {record_metadata.topic} "
                       f"[partition {record_metadata.partition}] "
                       f"offset {record_metadata.offset}")
            return True

        except KafkaError as e:
            logger.error(f"Failed to send message {message.id}: {e}")
            return False

    def close(self):
        """Закрытие продюсера с ожиданием завершения отправки"""
        self.producer.flush(timeout=10)
        self.producer.close()
        logger.info("Producer closed")

def main():
    producer = MessageProducer(
        bootstrap_servers='kafka-1:9092,kafka-2:9093,kafka-3:9094',
        topic='requests'
    )
    try:
        message_id = 1
        while True:
            # Создание тестового сообщения
            content = f"Test message {message_id}"
            # С небольшим шансом создаем сообщение с ошибкой для тестирования
            if random.random() < 0.1:  # 10% chance
                content = "Error message for testing"
            message = Message(id=message_id, content=content)
            # Отправка сообщения
            success = producer.send_message(message)
            if success:
                message_id += 1
            # Задержка между сообщениями
            time.sleep(2)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    finally:
        producer.close()

if __name__ == "__main__":
    main()