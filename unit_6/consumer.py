import signal
import sys
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import TopicAuthorizationFailedError
from concurrent.futures import ThreadPoolExecutor, as_completed

class GracefulShutdown:
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print(f"\nReceived shutdown signal")
        self.shutdown = True

def create_consumer_for_topic(topic_name):
    """Создает отдельный консьюмер для конкретного топика"""
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=["kafka-1:9093", "kafka-2:9093", "kafka-3:9093"],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=None,
            security_protocol='SSL',
            ssl_check_hostname=False,
            ssl_cafile='/app/kafka-client/ca.crt',
            ssl_certfile='/app/kafka-client/client.crt',
            ssl_keyfile='/app/kafka-client/client.key',
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            value_deserializer=lambda v: v.decode('utf-8') if v else None,
        )
        return topic_name, consumer, None
    except TopicAuthorizationFailedError:
        return topic_name, None, f"No access to topic: {topic_name}"
    except Exception as e:
        return topic_name, None, f"Error creating consumer for {topic_name}: {e}"

def consume_topic(topic_name, consumer, shutdown_flag):
    """Потоковая функция для потребления из одного топика"""
    print(f"Starting consumption from {topic_name}")

    try:
        for message in consumer:
            if shutdown_flag.shutdown:
                break
            print(f"[{topic_name}] {message.key}: {message.value}")
    except Exception as e:
        print(f"Error in consumer for {topic_name}: {e}")
    finally:
        consumer.close()
        print(f"Consumer for {topic_name} closed")

def main():
    shutdown_handler = GracefulShutdown()
    consumers = {}

    print("Starting Kafka consumers for topics: topic-1, topic-2")

    # Создаем консьюмеры для каждого топика параллельно
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(create_consumer_for_topic, 'topic-1'),
            executor.submit(create_consumer_for_topic, 'topic-2')
        ]

        for future in as_completed(futures):
            topic_name, consumer, error = future.result()
            if consumer:
                consumers[topic_name] = consumer
                print(f"✓ Consumer created for {topic_name}")
            elif error:
                print(f"✗ {error}")

    if not consumers:
        print("No consumers created. Exiting.")
        return

    # Запускаем потребление из всех доступных топиков параллельно
    print(f"\nStarting parallel consumption from {len(consumers)} topic(s)")

    with ThreadPoolExecutor(max_workers=len(consumers)) as executor:
        consumption_futures = [
            executor.submit(consume_topic, topic_name, consumer, shutdown_handler)
            for topic_name, consumer in consumers.items()
        ]

        try:
            # Ждем завершения всех потоков или сигнала остановки
            while not shutdown_handler.shutdown:
                # Проверяем завершились ли потоки
                all_done = all(future.done() for future in consumption_futures)
                if all_done:
                    break

                # Короткая пауза
                import time
                time.sleep(0.5)

        except KeyboardInterrupt:
            print("\nInterrupted")
            shutdown_handler.shutdown = True

        # Дожидаемся завершения потоков
        for future in consumption_futures:
            try:
                future.result(timeout=5)
            except Exception:
                pass

    print("All consumers stopped")

if __name__ == "__main__":
    main()