import os
import json
import time
import logging
import shutil
import socket
from pathlib import Path
from typing import List, Dict, Any, Set
from dataclasses import dataclass
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic_name: str
    ssl_enabled: bool
    ssl_config: Dict[str, str]
    processed_suffix: str = "_processed"

class DataPublisher:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer = self._create_producer()
        self.processed_files: Set[str] = set()
        
    def _create_producer(self) -> KafkaProducer:
        """Создает Kafka producer с SSL конфигурацией"""
        base_path = "/app/certs"
        producer_conf = {
            "bootstrap_servers": self.config.bootstrap_servers.split(','),
            "security_protocol": "SSL",
            "ssl_check_hostname": False,
            "ssl_cafile": f"{base_path}/ca.crt",
            "ssl_certfile": f"{base_path}/client.crt",
            "ssl_keyfile": f"{base_path}/client.key",
            "value_serializer": lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            "key_serializer": lambda k: str(k).encode('utf-8') if k else None,
            "acks": "all",
            "retries": 3,
            "max_in_flight_requests_per_connection": 1,
        }
        
        try:
            producer = KafkaProducer(**producer_conf)
            logger.info("Kafka producer created successfully")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def _check_connection(self) -> bool:
        """Проверяет подключение к Kafka (простая проверка)"""
        try:
            from kafka import KafkaAdminClient
            
            admin_config = {
                "bootstrap_servers": self.config.bootstrap_servers.split(','),
            }
            
            if self.config.ssl_enabled:
                base_path = "/app/certs"
                admin_config.update({
                    "security_protocol": "SSL",
                    "ssl_check_hostname": False,
                    "ssl_cafile": f"{base_path}/ca.crt",
                    "ssl_certfile": f"{base_path}/client.crt",
                    "ssl_keyfile": f"{base_path}/client.key",
                })
            
            admin_client = KafkaAdminClient(**admin_config)
            cluster_metadata = admin_client.list_topics()
            admin_client.close()
            
            logger.info(f"Successfully connected to Kafka cluster")
            logger.info(f"Available topics: {len(cluster_metadata)}")
            
            # Проверяем существует ли наш топик
            if self.config.topic_name in cluster_metadata:
                logger.info(f"Target topic '{self.config.topic_name}' exists")
            else:
                logger.warning(f"Target topic '{self.config.topic_name}' does not exist (will be created on first write)")
            
            return True
            
        except Exception as e:
            logger.error(f"Cannot connect to Kafka: {e}")
            logger.error("Please check:")
            logger.error("  1. Kafka brokers are running")
            logger.error("  2. SSL certificates are correctly mounted")
            logger.error("  3. Network connectivity between containers")
            return False
    
    def _convert_timestamp(self, timestamp_str: str) -> int:
        """Преобразует строку timestamp в миллисекунды"""
        try:
            if not timestamp_str:
                return int(time.time() * 1000)
            
            # Если уже число
            if isinstance(timestamp_str, (int, float)):
                return int(timestamp_str)
            
            # Если строка-число
            if isinstance(timestamp_str, str) and timestamp_str.replace('.', '', 1).isdigit():
                num = float(timestamp_str)
                if num < 10000000000:  # Секунды
                    return int(num * 1000)
                return int(num)
            
            # Преобразуем строку даты
            dt_str = timestamp_str.strip()
            if dt_str.endswith('Z'):
                dt_str = dt_str[:-1] + '+00:00'
            
            dt = datetime.fromisoformat(dt_str)
            return int(dt.timestamp() * 1000)
            
        except Exception as e:
            logger.error(f"Error converting timestamp '{timestamp_str}': {e}")
            return int(time.time() * 1000)
    
    def _process_data_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Обрабатывает один элемент данных для JSON"""
        item_copy = item.copy()
        
        # Обрабатываем timestamp поля
        for field in ['created_at', 'updated_at']:
            if field in item_copy:
                value = item_copy[field]
                if isinstance(value, str):
                    item_copy[field] = self._convert_timestamp(value)
                elif isinstance(value, (int, float)):
                    item_copy[field] = int(value)
                    if item_copy[field] < 10000000000:  # Проверяем секунды vs миллисекунды
                        item_copy[field] = item_copy[field] * 1000
        
        # Убедимся что все вложенные структуры корректны для JSON
        if 'price' in item_copy:
            if isinstance(item_copy['price'], dict):
                # Преобразуем amount к float
                if 'amount' in item_copy['price']:
                    try:
                        item_copy['price']['amount'] = float(item_copy['price']['amount'])
                    except:
                        item_copy['price']['amount'] = 0.0
                # Гарантируем наличие currency
                if 'currency' not in item_copy['price']:
                    item_copy['price']['currency'] = 'USD'
            else:
                # Если price не dict, создаем структуру по умолчанию
                try:
                    amount = float(item_copy['price'])
                    item_copy['price'] = {'amount': amount, 'currency': 'USD'}
                except:
                    item_copy['price'] = {'amount': 0.0, 'currency': 'USD'}
        
        if 'stock' in item_copy:
            if isinstance(item_copy['stock'], dict):
                for stock_field in ['available', 'reserved']:
                    if stock_field in item_copy['stock']:
                        try:
                            item_copy['stock'][stock_field] = int(item_copy['stock'][stock_field])
                        except:
                            item_copy['stock'][stock_field] = 0
                    else:
                        item_copy['stock'][stock_field] = 0
            else:
                # Если stock не dict, создаем структуру по умолчанию
                try:
                    available = int(item_copy['stock'])
                    item_copy['stock'] = {'available': available, 'reserved': 0}
                except:
                    item_copy['stock'] = {'available': 0, 'reserved': 0}
        
        # Гарантируем что tags и images - списки
        if 'tags' in item_copy and not isinstance(item_copy['tags'], list):
            if isinstance(item_copy['tags'], str):
                item_copy['tags'] = [item_copy['tags']]
            else:
                item_copy['tags'] = []
        
        if 'images' in item_copy and not isinstance(item_copy['images'], list):
            item_copy['images'] = []
        
        # Гарантируем что specifications - dict
        if 'specifications' in item_copy and not isinstance(item_copy['specifications'], dict):
            item_copy['specifications'] = {}
        
        return item_copy
    
    def read_data_files(self, data_dir: str) -> List[Dict[str, Any]]:
        """Читает все JSON файлы из директории"""
        data_files = []
        data_path = Path(data_dir)
        
        if not data_path.exists():
            logger.error(f"Data directory {data_dir} does not exist")
            return []
        
        for file_path in data_path.glob("*.json"):
            try:
                # Пропускаем уже обработанные файлы
                if str(file_path) in self.processed_files:
                    continue
                
                # Пропускаем файлы с суффиксом processed
                if self.config.processed_suffix in file_path.stem:
                    continue
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    if isinstance(data, list):
                        for item in data:
                            item['_source_file'] = file_path.name
                            data_files.append(item)
                    else:
                        data['_source_file'] = file_path.name
                        data_files.append(data)
                
                logger.info(f"Read {file_path.name} ({len(data_files)} total items)")
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {file_path}: {e}")
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
        
        return data_files
    
    def publish_to_kafka(self, data: List[Dict[str, Any]]) -> bool:
        """Публикует данные в Kafka топик в JSON формате"""
        if not data:
            logger.info("No data to publish")
            return True
        
        logger.info(f"Publishing {len(data)} records to '{self.config.topic_name}'")
        
        # Обрабатываем данные
        processed_data = []
        for i, item in enumerate(data):
            try:
                processed_item = self._process_data_item(item)
                processed_data.append(processed_item)
                
                # Логируем первый элемент для отладки
                if i == 0:
                    logger.info(f"First item sample:")
                    logger.info(f"  product_id: {processed_item.get('product_id')}")
                    logger.info(f"  created_at: {processed_item.get('created_at')}")
                    logger.info(f"  price: {processed_item.get('price')}")
                    
            except Exception as e:
                logger.error(f"Error processing item {item.get('product_id')}: {e}")
                processed_data.append(item)
        
        # Группируем по файлам
        files_data: Dict[str, List[Dict[str, Any]]] = {}
        published_files = set()
        success_count = 0
        error_count = 0
        
        for i, item in enumerate(processed_data):
            try:
                # Подготавливаем элемент для отправки
                item_to_send = item.copy()
                source_file = item_to_send.pop('_source_file', None)
                
                # Группировка для отметки файлов
                if source_file:
                    if source_file not in files_data:
                        files_data[source_file] = []
                    files_data[source_file].append(item_to_send.copy())
                    published_files.add(source_file)
                
                # Отправляем в Kafka
                self.producer.send(
                    topic=self.config.topic_name,
                    value=item_to_send,
                    key=str(item_to_send.get('product_id', '')).encode('utf-8')
                )
                
                success_count += 1
                
                # Логируем прогресс
                if success_count % 100 == 0:
                    logger.info(f"  Sent {success_count}/{len(processed_data)} messages...")
                
            except Exception as e:
                logger.error(f"Error publishing product {item.get('product_id')}: {e}")
                error_count += 1
        
        # Завершаем отправку
        try:
            self.producer.flush(timeout=15)
            logger.info(f"Flush completed")
            
        except Exception as e:
            logger.error(f"Flush failed: {e}")
            error_count += len(processed_data) - success_count
        
        # Проверяем результаты
        if error_count > 0:
            logger.error(f"=== PUBLICATION FAILED ===")
            logger.error(f"Errors: {error_count} out of {len(data)} messages failed")
            logger.error(f"Files will NOT be marked as processed!")
            return False
        
        logger.info(f"=== PUBLICATION SUCCESSFUL ===")
        logger.info(f"All {success_count} messages successfully delivered to '{self.config.topic_name}'")
        
        # Отмечаем файлы как обработанные
        self._mark_files_as_processed(published_files, files_data)
        return True
    
    def _mark_files_as_processed(self, published_files: Set[str], files_data: Dict[str, List[Dict[str, Any]]]):
        """Отмечает файлы как обработанные (только после успешной публикации)"""
        data_dir = self.config.ssl_config.get('data_dir', '/app/data')
        data_path = Path(data_dir)
        
        for filename in published_files:
            try:
                file_path = data_path / filename
                if file_path.exists():
                    # Создаем backup с метаданными
                    backup_data = {
                        'original_file': filename,
                        'processed_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                        'records_published': len(files_data.get(filename, [])),
                        'topic': self.config.topic_name,
                        'format': 'JSON',
                    }
                    
                    backup_file = data_path / f"{file_path.stem}{self.config.processed_suffix}_backup.json"
                    with open(backup_file, 'w', encoding='utf-8') as f:
                        json.dump(backup_data, f, ensure_ascii=False, indent=2)
                    
                    # Переименовываем файл
                    new_name = file_path.stem + self.config.processed_suffix + file_path.suffix
                    new_path = data_path / new_name
                    file_path.rename(new_path)
                    
                    self.processed_files.add(str(file_path))
                    logger.info(f"✓ File marked as processed: {filename}")
                else:
                    logger.warning(f"File not found: {filename}")
                    
            except Exception as e:
                logger.error(f"Error marking file {filename} as processed: {e}")
    
    def _move_processed_files(self, data_path: Path, processed_dir: Path):
        """Перемещает уже обработанные файлы (с суффиксом) в поддиректорию"""
        try:
            # Ищем файлы с суффиксом processed
            processed_files = list(data_path.glob(f"*{self.config.processed_suffix}*"))
            
            for file_path in processed_files:
                try:
                    new_path = processed_dir / file_path.name
                    shutil.move(str(file_path), str(new_path))
                    logger.debug(f"Moved {file_path.name} to processed directory")
                except Exception as e:
                    logger.warning(f"Could not move file {file_path.name}: {e}")
        except Exception as e:
            logger.error(f"Error moving processed files: {e}")
    
    def run(self):
        """Основной метод запуска публикатора"""
        logger.info("=" * 60)
        logger.info("Starting Data Publisher (JSON format)")
        logger.info("=" * 60)
        logger.info(f"Configuration:")
        logger.info(f"  Bootstrap servers: {self.config.bootstrap_servers}")
        logger.info(f"  Topic: {self.config.topic_name}")
        logger.info(f"  SSL enabled: {self.config.ssl_enabled}")
        logger.info(f"  Data directory: {self.config.ssl_config.get('data_dir', '/app/data')}")
        logger.info(f"  Output format: JSON")
        
        # Проверяем подключение к Kafka
        logger.info("-" * 60)
        logger.info("Checking Kafka connection...")
        if not self._check_connection():
            logger.error("Kafka connection check failed.")
            logger.error("Will retry in 60 seconds...")
            time.sleep(60)
            # Можно попробовать снова или выйти
            if not self._check_connection():
                logger.error("Failed to connect to Kafka after retry. Exiting.")
                return
        
        data_dir = self.config.ssl_config.get('data_dir', '/app/data')
        data_path = Path(data_dir)
        
        # Создаем поддиректорию для обработанных файлов
        processed_dir = data_path / "processed"
        processed_dir.mkdir(exist_ok=True)
        
        logger.info(f"Watching directory: {data_path}")
        logger.info(f"Processed files will be moved to: {processed_dir}")
        logger.info("-" * 60)
        
        while True:
            try:
                # Читаем данные из директории
                data = self.read_data_files(data_dir)
                
                if data:
                    logger.info(f"Found {len(data)} records to publish")
                    
                    # Публикуем данные в Kafka
                    success = self.publish_to_kafka(data)
                    
                    # Только при успешной публикации перемещаем файлы
                    if success:
                        self._move_processed_files(data_path, processed_dir)
                        logger.info("✓ Publication completed successfully")
                    else:
                        logger.warning("✗ Publication failed. Files will remain for retry.")
                        
                    logger.info("-" * 60)
                else:
                    logger.info("No new files to process. Waiting 30 seconds...")
                
                # Ожидание перед следующей проверкой
                time.sleep(30)
                
            except KeyboardInterrupt:
                logger.info("Shutdown requested by user...")
                break
            except Exception as e:
                logger.error(f"Error in main run loop: {e}")
                logger.error(f"Waiting 10 seconds before retry...")
                time.sleep(10)

def main():
    """Точка входа в приложение"""
    # Конфигурация из переменных окружения
    config = KafkaConfig(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:9093,kafka-2:9095,kafka-3:9097'),
        topic_name=os.getenv('TOPIC_NAME', 'shop-raw'),
        ssl_enabled=os.getenv('SSL_ENABLED', 'true').lower() == 'true',
        ssl_config={
            'data_dir': os.getenv('DATA_DIR', '/app/data')
        },
        processed_suffix=os.getenv('PROCESSED_SUFFIX', '_processed')
    )
    
    logger.info(f"Using bootstrap servers: {config.bootstrap_servers}")
    
    # Создаем и запускаем публикатор
    publisher = DataPublisher(config)
    
    try:
        publisher.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if hasattr(publisher, 'producer'):
            publisher.producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    main()