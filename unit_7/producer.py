#!/usr/bin/env python3

import json
import time
import signal
from confluent_kafka import Producer, KafkaError

class ConfluentKafkaProducer:
    def __init__(self, topic_name='test-topic'):
        self.topic = topic_name
        self.running = True
        self.message_count = 0
        
        # Обработчик сигнала
        signal.signal(signal.SIGINT, self.signal_handler)
        
        self.config = {
            'bootstrap.servers': 'rc1a-b3q1bldvbed6jut7.mdb.yandexcloud.net:9091,'
                                'rc1b-gcsvvf5c7inos250.mdb.yandexcloud.net:9091,'
                                'rc1d-g758v4bbauofgqa1.mdb.yandexcloud.net:9091',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-512',
            'sasl.username': 'admin_user',
            'sasl.password': 'superpuperpass',
            'ssl.ca.location': '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt',
            # Дополнительные настройки для надежности
            'acks': 'all',
            'retries': 3,
            # 'compression.type': 'gzip',
            'client.id': 'python-producer-confluent'
        }
        
        # Создаем producer
        self.producer = Producer(self.config)
        
        print(f"[INFO] Confluent Producer инициализирован для топика: {self.topic}")
        print(f"[INFO] Используются те же настройки что и в kcat")
    
    def signal_handler(self, signum, frame):
        print(f"\n[INFO] Завершение работы...")
        self.running = False
    
    def delivery_report(self, err, msg):
        """Callback вызывается при доставке или ошибке"""
        if err is not None:
            print(f'[ERROR] Доставка не удалась: {err}')
        else:
            self.message_count += 1
            print(f'[SUCCESS] Сообщение #{self.message_count} доставлено: '
                  f'{msg.topic()} [{msg.partition()}] @ {msg.offset()}')
    
    def run(self):
        print("[INFO] Начинаем отправку. Ctrl+C для остановки")
        
        msg_id = 1
        
        try:
            while self.running:
                # Создаем сообщение
                message = {
                    "id": msg_id,
                    "text": f"Test message from confluent producer #{msg_id}",
                    "timestamp": int(time.time()),
                    "source": "confluent-python"
                }
                
                # Конвертируем в JSON
                message_json = json.dumps(message)
                
                print(f"[PRODUCE] Отправляем: {message_json}")
                
                # Отправляем асинхронно с callback
                self.producer.produce(
                    topic=self.topic,
                    key=str(msg_id),
                    value=message_json.encode('utf-8'),
                    callback=self.delivery_report
                )
                
                # Периодически опрашиваем для вызова callbacks
                self.producer.poll(0)
                
                msg_id += 1
                time.sleep(2)
        
        except KeyboardInterrupt:
            print("\n[INFO] Прервано пользователем")
        except Exception as e:
            print(f"[ERROR] Ошибка: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Завершение работы"""
        print(f"\n[INFO] Итого отправлено: {self.message_count}")
        
        # Ждем доставки всех сообщений
        self.producer.flush(timeout=10)
        print("[INFO] Producer остановлен")

if __name__ == "__main__":
    producer = ConfluentKafkaProducer(topic_name='test-topic')
    producer.run()