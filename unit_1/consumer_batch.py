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
logger = logging.getLogger('batch_consumer')

class BatchMessageConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, batch_size: int = 10):
        self.instance_id = INSTANCE_ID
        self.batch_size = batch_size
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id='batch-message-group',  # Уникальный group_id
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Ручное управление коммитами
            # Десериализаторы
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            value_deserializer=MessageSerializer.deserialize_json,
            # Настройки для батчевого чтения
            fetch_max_wait_ms=5000,  # Максимальное время ожидания данных (5 сек)
            fetch_min_bytes=1024,  # Минимальный размер данных (1 KB)
            max_poll_records=batch_size,  # Максимум batch_size сообщений за poll
            max_partition_fetch_bytes=1048576,  # 1 MB на партицию
            # Настройки надежности
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        logger.info(f"Batch consumer initialized (batch_size={batch_size})")

    def consume_messages(self):
        """Потребление сообщений батчами"""
        try:
            while True:
                # Получение батча сообщений
                batch = self.consumer.poll(timeout_ms=10000, max_records=self.batch_size)

                if not batch:
                    logger.debug("No messages received in this poll")
                    continue

                processed_count = 0
                failed_count = 0

                # Обработка всех сообщений в батче
                for topic_partition, messages in batch.items():
                    logger.info(f"Processing batch from {topic_partition} "
                               f"with {len(messages)} messages")

                    for message in messages:
                        try:
                            # Вывод полученного сообщения
                            print(f"[{self.instance_id}] Received batch message: {message.value}")

                            # Обработка сообщения
                            success = MessageProcessor.process_message(message.value)

                            if success:
                                processed_count += 1
                                logger.info(f"Successfully processed message {message.value.id}")
                            else:
                                failed_count += 1
                                logger.warning(f"Failed to process message {message.value.id}")

                        except Exception as e:
                            failed_count += 1
                            logger.error(f"Error processing message {message.value.id}: {e}")
                            # Продолжаем обработку остальных сообщений

                # Коммит оффсетов после обработки всего батча
                if processed_count > 0 or failed_count > 0:
                    try:
                        self.consumer.commit()
                        logger.info(f"Committed offsets. Processed: {processed_count}, "
                                  f"Failed: {failed_count}")
                    except Exception as e:
                        logger.error(f"Error committing offsets: {e}")

        except KafkaError as e:
            logger.error(f"Kafka error in batch consumer: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in batch consumer: {e}")
        finally:
            self.close()

    def close(self):
        """Закрытие консьюмера"""
        self.consumer.close()
        logger.info("Batch consumer closed")

def main():
    consumer = BatchMessageConsumer(
        bootstrap_servers='kafka-1:9092,kafka-2:9093,kafka-3:9094',
        topic='requests',
        batch_size=10
    )

    try:
        consumer.consume_messages()
    except KeyboardInterrupt:
        logger.info("Batch consumer interrupted by user")

if __name__ == "__main__":
    main()