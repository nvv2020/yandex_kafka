import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from message import MessageSerializer, MessageProcessor
import os
import socket

INSTANCE_ID = os.getenv('HOSTNAME', socket.gethostname())

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('single_consumer')

class SingleMessageConsumer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.instance_id = INSTANCE_ID
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id='single-message-group',  # Уникальный group_id
            auto_offset_reset='earliest',  # Начать с самого старого сообщения
            enable_auto_commit=True,  # Автоматический коммит оффсетов
            auto_commit_interval_ms=1000,
            # Десериализатор json
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            value_deserializer=MessageSerializer.deserialize_json,
            # Настройки для чтения по одному сообщению
            fetch_max_wait_ms=100,  # Максимальное время ожидания данных
            fetch_min_bytes=1,  # Минимальный размер данных для возврата
            max_poll_records=1,  # Максимум одно сообщение за poll
            # Настройки надежности
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        logger.info("Single message consumer initialized")

    def consume_messages(self):
        """Потребление сообщений по одному"""
        try:
            for message in self.consumer:
                try:
                    # Вывод полученного сообщения
                    print(f"[{self.instance_id}] Received single message: {message.value}")

                    # Обработка сообщения
                    success = MessageProcessor.process_message(message.value)

                    if success:
                        logger.info(f"Successfully processed message {message.value.id}")
                    else:
                        logger.warning(f"Failed to process message {message.value.id}")

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Продолжаем работу после ошибки

        except KafkaError as e:
            logger.error(f"Kafka error in consumer: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {e}")
        finally:
            self.close()

    def close(self):
        """Закрытие консьюмера"""
        self.consumer.close()
        logger.info("Single message consumer closed")

def main():
    consumer = SingleMessageConsumer(
        bootstrap_servers='kafka-1:9092,kafka-2:9093,kafka-3:9094',
        topic='requests'
    )

    try:
        consumer.consume_messages()
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")

if __name__ == "__main__":
    main()