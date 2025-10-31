#!/usr/bin/env python3
"""
Consumer для чтения данных из Kafka топиков Debezium
Использует confluent-kafka-python (официальная библиотека от Confluent)
"""

from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import os
import sys
from dotenv import load_dotenv
import signal
import time

# Загрузка переменных окружения из .env файла
load_dotenv()

class DebeziumConsumer:
    def __init__(self):
        self.consumer = None
        self.running = True

        # Обработка сигнала Ctrl+C для graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        print(f"\n📭 Получен сигнал остановки. Завершаем работу...")
        self.running = False

    def _load_config(self):
        """Загрузка конфигурации из .env файла"""
        try:
            # Получение bootstrap servers из .env
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9094')

            # Получение топиков из .env
            topics = os.getenv('KAFKA_TOPICS', 'customers.public.users,customers.public.orders')
            topics = [topic.strip() for topic in topics.split(',')]

            # Дополнительные настройки
            group_id = os.getenv('KAFKA_CONSUMER_GROUP', 'python-debezium-consumer')
            auto_offset_reset = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')

            config = {
                'bootstrap_servers': bootstrap_servers,
                'topics': topics,
                'group_id': group_id,
                'auto_offset_reset': auto_offset_reset
            }

            print("⚙️ Загружена конфигурация:")
            print(f"   Bootstrap Servers: {bootstrap_servers}")
            print(f"   Topics: {topics}")
            print(f"   Consumer Group: {group_id}")
            print(f"   Auto Offset Reset: {auto_offset_reset}")

            return config

        except Exception as e:
            print(f"❌ Ошибка загрузки конфигурации: {e}")
            sys.exit(1)

    def _create_consumer(self, config):
        """Создание Kafka consumer"""
        try:
            consumer_config = {
                'bootstrap.servers': config['bootstrap_servers'],
                'group.id': config['group_id'],
                'auto.offset.reset': config['auto_offset_reset'],
                'enable.auto.commit': True,
                'auto.commit.interval.ms': 5000,
                'session.timeout.ms': 30000,
                'max.poll.interval.ms': 300000
            }

            consumer = Consumer(consumer_config)

            # Подписка на топики
            consumer.subscribe(config['topics'])
            print(f"✅ Успешно подключены к Kafka и подписались на топики")
            return consumer

        except KafkaException as e:
            print(f"❌ Ошибка создания consumer: {e}")
            print("   Проверьте настройки KAFKA_BOOTSTRAP_SERVERS в .env файле")
            raise
        except Exception as e:
            print(f"❌ Неожиданная ошибка создания consumer: {e}")
            raise

    def _deserialize_message(self, message_value):
        """Десериализация сообщения из JSON"""
        if not message_value:
            return None

        try:
            return json.loads(message_value.decode('utf-8'))
        except json.JSONDecodeError as e:
            print(f"⚠️ Ошибка декодирования JSON: {e}")
            return {"raw_data": message_value.hex()}
        except Exception as e:
            print(f"⚠️ Ошибка десериализации: {e}")
            return None

    def _format_debezium_message(self, data):
        """Форматирование сообщения Debezium для красивого вывода"""
        if not data:
            return "Пустое сообщение"

        # Проверяем, это ли сообщение Debezium CDC
        if isinstance(data, dict) and 'op' in data:
            output = []
            output.append(f"🔸 Операция: {self._get_operation_name(data['op'])}")

            if 'before' in data and data['before']:
                output.append("📥 Данные ДО изменения:")
                output.append(json.dumps(data['before'], indent=2, ensure_ascii=False))

            if 'after' in data and data['after']:
                output.append("📤 Данные ПОСЛЕ изменения:")
                output.append(json.dumps(data['after'], indent=2, ensure_ascii=False))

            if 'source' in data:
                output.append(f"📋 Источник: {data['source'].get('table', 'N/A')} "
                            f"(lsn: {data['source'].get('lsn', 'N/A')})")

            if 'ts_ms' in data:
                from datetime import datetime
                timestamp = datetime.fromtimestamp(data['ts_ms'] / 1000)
                output.append(f"🕐 Время: {timestamp}")

            return '\n'.join(output)
        else:
            # Простое JSON сообщение
            return json.dumps(data, indent=2, ensure_ascii=False)

    def _get_operation_name(self, op_code):
        """Получение читаемого имени операции"""
        operations = {
            'c': 'CREATE (вставка)',
            'u': 'UPDATE (обновление)',
            'd': 'DELETE (удаление)',
            'r': 'READ (чтение)',
            't': 'TRUNCATE (очистка)'
        }
        return operations.get(op_code, f'Unknown ({op_code})')

    def consume_messages(self):
        """Основной метод для чтения сообщений"""
        config = self._load_config()

        try:
            self.consumer = self._create_consumer(config)

            print(f"\n🚀 Начинаем чтение топиков Debezium")
            print("=" * 60)
            print("Нажмите Ctrl+C для остановки\n")

            message_count = 0
            last_activity_time = time.time()
            empty_polls = 0

            while self.running:
                try:
                    # Poll с таймаутом
                    msg = self.consumer.poll(timeout=1.0)

                    if msg is None:
                        # Нет сообщений
                        empty_polls += 1
                        if empty_polls % 30 == 0:  # Каждые 30 пустых опросов
                            print("⏳ Ожидаем сообщения... (топики могут быть пустыми или коннектор не настроен)")
                        continue

                    if msg.error():
                        # Обработка ошибок Kafka
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # Достигнут конец партиции
                            continue
                        else:
                            print(f"❌ Ошибка Kafka: {msg.error()}")
                            continue

                    # Успешное сообщение
                    empty_polls = 0
                    message_count += 1
                    last_activity_time = time.time()

                    self._print_message(msg, message_count)

                except KeyboardInterrupt:
                    print("\n⏹️ Остановка по запросу пользователя...")
                    break
                except Exception as e:
                    print(f"⚠️ Неожиданная ошибка: {e}")
                    time.sleep(1)

        except Exception as e:
            print(f"❌ Критическая ошибка: {e}")
        finally:
            self._cleanup()

    def _print_message(self, msg, message_count):
        """Вывод сообщения в консоль"""
        print(f"\n📨 Сообщение #{message_count}")
        print(f"🔖 Топик: {msg.topic()}")
        print(f"📦 Партиция: {msg.partition()}")
        print(f"📍 Смещение: {msg.offset()}")
        if msg.key():
            print(f"🔑 Ключ: {msg.key().decode('utf-8')}")

        message_data = self._deserialize_message(msg.value())
        formatted_data = self._format_debezium_message(message_data)
        print("📄 Данные:")
        print(formatted_data)
        print("-" * 60)

    def _cleanup(self):
        """Очистка ресурсов"""
        if self.consumer:
            try:
                self.consumer.close()
                print("✅ Consumer закрыт")
            except Exception as e:
                print(f"⚠️ Ошибка при закрытии consumer: {e}")

def main():
    """Основная функция"""
    print("🎯 Debezium Kafka Consumer (confluent-kafka-python)")
    print("=" * 50)

    # Проверяем наличие необходимых переменных
    if not os.path.exists('.env'):
        print("⚠️ Файл .env не найден. Использую значения по умолчанию.")
        print("   Создайте .env файл для кастомизации настроек")

    consumer = DebeziumConsumer()
    consumer.consume_messages()

if __name__ == "__main__":
    main()