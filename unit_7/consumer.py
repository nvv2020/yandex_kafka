#!/usr/bin/env python3
"""
Kafka Consumer на confluent-kafka с настройками как в kcat
Читает сообщения до получения Ctrl+C
"""
import json
import signal
import sys
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException

class ConfluentKafkaConsumer:
    def __init__(self, topic_name='test-topic'):
        self.topic = topic_name
        self.running = True
        self.message_count = 0
        
        # Обработчик сигнала для graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Конфигурация как в работающей команде kcat
        self.config = {
            'bootstrap.servers': 'rc1a-b3q1bldvbed6jut7.mdb.yandexcloud.net:9091,'
                                'rc1b-gcsvvf5c7inos250.mdb.yandexcloud.net:9091,'
                                'rc1d-g758v4bbauofgqa1.mdb.yandexcloud.net:9091',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-512',
            'sasl.username': 'admin_user',
            'sasl.password': 'superpuperpass',
            'ssl.ca.location': '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt',
            
            # Конфигурация consumer
            'group.id': 'python-confluent-consumer-group',
            'auto.offset.reset': 'earliest',  # начинать с начала если нет offset
            'enable.auto.commit': True,        # авто-коммит offset
            'enable.auto.offset.store': False, # ручное сохранение offset
            
            # Настройки для надежности
            'session.timeout.ms': 10000,       # 10 секунд
            'max.poll.interval.ms': 300000,    # 5 минут
            'heartbeat.interval.ms': 3000,     # 3 секунды
            
            # Настройки потребления
            'fetch.min.bytes': 1,
            'fetch.max.bytes': 52428800,       # 50 MB
            'fetch.wait.max.ms': 500,
            'client.id': 'python-confluent-consumer'
        }
        
        # Создаем consumer
        self.consumer = Consumer(self.config)
        
        # Подписываемся на топик
        self.consumer.subscribe([self.topic], 
                               on_assign=self.on_assign,
                               on_revoke=self.on_revoke,
                               on_lost=self.on_lost)
        
        print(f"[INFO] Confluent Consumer инициализирован")
        print(f"[INFO] Топик: {self.topic}")
        print(f"[INFO] Group ID: {self.config['group.id']}")
        print("[INFO] Ожидание сообщений...")
    
    def signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        print(f"\n[INFO] Получен сигнал {signum}, завершаем работу...")
        self.running = False
    
    def on_assign(self, consumer, partitions):
        """Callback при назначении партиций"""
        print(f"[ASSIGN] Назначены партиции: {[p.partition for p in partitions]}")
    
    def on_revoke(self, consumer, partitions):
        """Callback при отзыве партиций"""
        print(f"[REVOKE] Отозваны партиции: {[p.partition for p in partitions]}")
    
    def on_lost(self, consumer, partitions):
        """Callback при потере партиций"""
        print(f"[LOST] Потеряны партиции: {[p.partition for p in partitions]}")
    
    def format_timestamp(self, timestamp_ms):
        """Форматирует timestamp в читаемый вид"""
        if timestamp_ms:
            return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        return "N/A"
    
    def try_parse_json(self, value_bytes):
        """Пытается распарсить сообщение как JSON"""
        try:
            if value_bytes:
                return json.loads(value_bytes.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        return value_bytes
    
    def print_message(self, msg):
        """Красиво выводит сообщение"""
        self.message_count += 1
        
        # Парсим значения
        key = msg.key().decode('utf-8') if msg.key() else None
        value = self.try_parse_json(msg.value())
        
        # Преобразуем значение для вывода
        if isinstance(value, dict):
            value_str = json.dumps(value, indent=2, ensure_ascii=False)
        elif isinstance(value, bytes):
            value_str = value.decode('utf-8', errors='replace')
        else:
            value_str = str(value)
        
        # Выводим сообщение
        print("\n" + "="*70)
        print(f"📨 СООБЩЕНИЕ #{self.message_count}")
        print("="*70)
        print(f"📌 Топик:        {msg.topic()}")
        print(f"🔢 Раздел:       {msg.partition()}")
        print(f"📍 Смещение:     {msg.offset()}")
        print(f"🔑 Ключ:         {key}")
        print(f"🆔 Group:        {self.config['group.id']}")
        
        if msg.timestamp():
            ts_type, ts_ms = msg.timestamp()
            ts_types = {0: "Unknown", 1: "Create", 2: "LogAppend"}
            print(f"🕒 Время ({ts_types.get(ts_type, ts_type)}): {self.format_timestamp(ts_ms)}")
        
        print(f"📏 Размер:       {len(msg.value()) if msg.value() else 0} байт")
        print("-"*70)
        print("📦 СОДЕРЖИМОЕ:")
        print(value_str[:500])  # Ограничиваем вывод
        if len(value_str) > 500:
            print(f"... (еще {len(value_str) - 500} символов)")
        print("="*70)
        
        # Сохраняем offset вручную
        self.consumer.store_offsets(msg)
    
    def consume_messages(self):
        """Основной цикл потребления сообщений"""
        print("[INFO] Начинаем чтение сообщений. Нажмите Ctrl+C для остановки.")
        
        try:
            while self.running:
                try:
                    # Получаем сообщение с таймаутом
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # Достигнут конец раздела - это нормально
                            print(f"[INFO] Конец раздела {msg.partition()}, продолжаем...")
                            continue
                        else:
                            print(f"[ERROR] Ошибка при получении: {msg.error()}")
                            # Для некоторых ошибок можно продолжать
                            if msg.error().retriable():
                                continue
                            else:
                                break
                    
                    # Обрабатываем сообщение
                    self.print_message(msg)
                    
                    # Периодически коммитим оффсеты
                    if self.message_count % 10 == 0:
                        self.consumer.commit(asynchronous=False)
                        print(f"[INFO] Закоммичены оффсеты после {self.message_count} сообщений")
                
                except KeyboardInterrupt:
                    print("\n[INFO] Получен KeyboardInterrupt")
                    break
                except KafkaException as e:
                    print(f"[KAFKA ERROR] {e}")
                    if e.args[0].code() in (KafkaError._ALL_BROKERS_DOWN, 
                                           KafkaError._TRANSPORT):
                        print("[ERROR] Потеряно соединение с Kafka")
                        break
                except Exception as e:
                    print(f"[ERROR] Неожиданная ошибка: {type(e).__name__}: {e}")
                    import traceback
                    traceback.print_exc()
                    # Продолжаем работу, но делаем паузу
                    import time
                    time.sleep(5)
        
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown"""
        print(f"\n[INFO] Завершение работы. Всего прочитано сообщений: {self.message_count}")
        
        try:
            # Коммитим последние оффсеты
            offsets = self.consumer.commit(asynchronous=False)
            if offsets:
                print(f"[INFO] Закоммичены оффсеты: {offsets}")
        except Exception as e:
            print(f"[WARNING] Ошибка при коммите оффсетов: {e}")
        
        try:
            # Отписываемся и закрываем consumer
            self.consumer.unsubscribe()
            self.consumer.close(timeout=5)
            print("[INFO] Consumer успешно остановлен")
        except Exception as e:
            print(f"[WARNING] Ошибка при остановке consumer: {e}")

def print_help():
    """Выводит справку по использованию"""
    print("="*60)
    print("Kafka Consumer для Yandex Managed Kafka")
    print("="*60)
    print("Использование:")
    print("  python3 consumer.py [topic_name]")
    print()
    print("Аргументы:")
    print("  topic_name - имя топика (по умолчанию: test-topic)")
    print()
    print("Примеры:")
    print("  python3 consumer.py")
    print("  python3 consumer.py my-topic")
    print("  python3 consumer.py test-topic")
    print("="*60)

if __name__ == "__main__":
    # Обработка аргументов командной строки
    topic = 'test-topic'
    
    if len(sys.argv) > 1:
        if sys.argv[1] in ('-h', '--help'):
            print_help()
            sys.exit(0)
        else:
            topic = sys.argv[1]
    
    # Создаем и запускаем consumer
    consumer = ConfluentKafkaConsumer(topic_name=topic)
    consumer.consume_messages()