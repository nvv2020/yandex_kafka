import os
import json
import logging
import ssl
from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import uuid4
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition, KafkaException

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Модели
class SearchRequest(BaseModel):
    client_id: Optional[str] = Field(None, description="ID клиента (опционально)")
    search_words: List[str] = Field(..., description="Список слов для поиска")

class RecommendationRequest(BaseModel):
    client_id: Optional[str] = Field(None, description="ID клиента (опционально)")

# FastAPI app
app = FastAPI(
    title="Client Search API",
    description="API для приема поисковых запросов от клиентов и получения рекомендаций",
    version="1.0.0"
)

# Конфигурация из переменных окружения
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka-1:9093,kafka-2:9095,kafka-3:9097')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
CLIENT_SEARCHES_TOPIC = os.getenv('CLIENT_SEARCHES_TOPIC', 'client-searches')
RECOMMENDATION_BROKERS = os.getenv('RECOMMENDATION_BROKERS', 'kafka-recommendations:9092')
RECOMMENDATIONS_TOPIC = os.getenv('RECOMMENDATIONS_TOPIC', 'client-recommendations')
SSL_CA = os.getenv('KAFKA_SSL_CA', '/app/certs/ca.crt')
SSL_CERT = os.getenv('KAFKA_SSL_CERT', '/app/certs/client.crt')
SSL_KEY = os.getenv('KAFKA_SSL_KEY', '/app/certs/client.key')

# Глобальные переменные
search_producer = None
schema_registry_client = None
avro_serializer = None
client_sessions = {}

def create_ssl_context():
    """Создание SSL контекста для подключения к основному Kafka"""
    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=SSL_CA
    )
    ssl_context.load_cert_chain(
        certfile=SSL_CERT,
        keyfile=SSL_KEY
    )
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    return ssl_context

def delivery_report(err, msg):
    """Callback для отчетов о доставке сообщений"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def create_client_session(client_id: Optional[str] = None) -> str:
    """Создание или получение сессии клиента"""
    if client_id and client_id in client_sessions:
        client_sessions[client_id]["last_activity"] = datetime.utcnow().isoformat()
        return client_id
    
    new_client_id = client_id or f"client_{str(uuid4())[:8]}"
    
    client_sessions[new_client_id] = {
        "created_at": datetime.utcnow().isoformat(),
        "last_activity": datetime.utcnow().isoformat(),
        "search_count": 0,
        "recommendation_requests": 0,
        "session_id": str(uuid4())[:16]
    }
    
    logger.info(f"Created new session for client: {new_client_id}")
    return new_client_id

def create_recommendation_consumer(read_from_beginning: bool = False):
    """Создание consumer для чтения рекомендаций"""
    consumer_config = {
        'bootstrap.servers': RECOMMENDATION_BROKERS,
        'group.id': f'client-api-recommendations-{str(uuid4())[:8]}',
        'auto.offset.reset': 'earliest' if read_from_beginning else 'latest',
        'enable.auto.commit': False,
        'session.timeout.ms': 10000,
        'max.poll.interval.ms': 300000,
        'fetch.wait.max.ms': 100,
        'fetch.min.bytes': 1,
        'fetch.max.bytes': 52428800,
        'enable.partition.eof': True,  # Включаем получение EOF событий
        'debug': 'consumer,broker'  # Включаем отладку
    }
    
    logger.info(f"Creating consumer with config: {consumer_config}")
    
    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe([RECOMMENDATIONS_TOPIC])
        
        # Ждем присвоения партиций
        assignment_start = datetime.utcnow()
        while True:
            if (datetime.utcnow() - assignment_start).total_seconds() > 10:
                logger.warning("Timeout waiting for partition assignment")
                break
            
            assignment = consumer.assignment()
            if assignment:
                logger.info(f"Consumer assigned to partitions: {assignment}")
                break
            
            consumer.poll(0.1)
        
        return consumer
        
    except Exception as e:
        logger.error(f"Failed to create consumer: {e}", exc_info=True)
        raise

@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    global search_producer, schema_registry_client, avro_serializer
    
    try:
        # 1. Инициализация Schema Registry
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        logger.info(f"Schema Registry client initialized: {SCHEMA_REGISTRY_URL}")
        
        # 2. Настройка Avro сериализатора
        schema_str = """{
            "type": "record",
            "name": "ClientSearch",
            "fields": [
                {"name": "client_id", "type": "string"},
                {"name": "search_words", "type": {"type": "array", "items": "string"}},
                {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }"""
        
        avro_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema_str,
            to_dict=lambda obj, ctx: obj
        )
        
        # 3. Producer для поисковых запросов (с SSL)
        producer_config = {
            'bootstrap.servers': KAFKA_BROKERS,
            'security.protocol': 'SSL',
            'ssl.ca.location': SSL_CA,
            'ssl.certificate.location': SSL_CERT,
            'ssl.key.location': SSL_KEY,
            'message.max.bytes': 1000000,
            'linger.ms': 5,
            'acks': 'all'
        }
        
        search_producer = Producer(producer_config)
        logger.info(f"Kafka producer created for brokers: {KAFKA_BROKERS}")
        logger.info("All connections initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize connections: {e}", exc_info=True)
        search_producer = None

@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при завершении"""
    if search_producer:
        search_producer.flush(timeout=5)
        logger.info("Kafka producer flushed")

@app.middleware("http")
async def client_session_middleware(request: Request, call_next):
    """Middleware для автоматического управления сессиями клиентов"""
    client_id = request.query_params.get("client_id") or request.headers.get("X-Client-ID")
    
    if request.url.path in ["/", "/health", "/debug", "/test_recommendations"]:
        return await call_next(request)
    
    client_id = create_client_session(client_id)
    request.state.client_id = client_id
    response = await call_next(request)
    response.headers["X-Client-ID"] = client_id
    
    return response

@app.get("/")
async def root():
    return {
        "service": "client-search-api",
        "version": "1.0.0",
        "endpoints": {
            "POST /search": "Отправка поискового запроса (Avro формат)",
            "GET /recommendations": "Получение рекомендаций",
            "GET /test_recommendations": "Тестовое чтение всех сообщений",
            "POST /test/send_recommendation": "Тестовая отправка рекомендации",
            "GET /debug": "Отладка Kafka",
            "GET /health": "Проверка состояния"
        }
    }

@app.get("/health")
async def health_check():
    status = {
        "service": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "kafka_producer": "connected" if search_producer else "disconnected",
        "active_sessions": len(client_sessions)
    }
    
    if not search_producer:
        raise HTTPException(status_code=503, detail=status)
    
    return status

@app.get("/debug")
async def debug_kafka():
    """Endpoint для отладки Kafka соединений"""
    try:
        # Создаем временный consumer для проверки
        consumer_conf = {
            'bootstrap.servers': RECOMMENDATION_BROKERS,
            'group.id': f'debug-consumer-{uuid4()}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([RECOMMENDATIONS_TOPIC])
        
        # Ждем assignment
        consumer.poll(1.0)
        
        # Получаем метаданные
        metadata = consumer.list_topics(timeout=5)
        
        topic_info = {}
        if RECOMMENDATIONS_TOPIC in metadata.topics:
            topic_metadata = metadata.topics[RECOMMENDATIONS_TOPIC]
            
            partitions = []
            total_messages = 0
            
            for partition_id in topic_metadata.partitions:
                try:
                    tp = TopicPartition(RECOMMENDATIONS_TOPIC, partition_id)
                    low, high = consumer.get_watermark_offsets(tp, timeout=5)
                    
                    partitions.append({
                        "partition": partition_id,
                        "low_watermark": low,
                        "high_watermark": high,
                        "messages_available": high - low
                    })
                    
                    total_messages += (high - low)
                    
                except Exception as e:
                    partitions.append({
                        "partition": partition_id,
                        "error": str(e)
                    })
            
            topic_info = {
                "exists": True,
                "partition_count": len(topic_metadata.partitions),
                "total_messages": total_messages,
                "partitions": partitions
            }
        else:
            topic_info = {
                "exists": False,
                "error": f"Topic '{RECOMMENDATIONS_TOPIC}' not found"
            }
        
        consumer.close()
        
        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "kafka_brokers": RECOMMENDATION_BROKERS,
            "topic": RECOMMENDATIONS_TOPIC,
            "topic_info": topic_info,
            "test_connection": "OK" if topic_info.get("exists") else "FAILED"
        }
        
    except Exception as e:
        logger.error(f"Debug error: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/test_recommendations")
async def test_recommendations(
    client_id: str = Query("123"),
    max_messages: int = Query(100)
):
    """Тестовый endpoint для чтения ВСЕХ сообщений из топика"""
    logger.info(f"Test reading ALL messages for client {client_id}")
    
    try:
        # Создаем consumer который читает с начала
        consumer_conf = {
            'bootstrap.servers': RECOMMENDATION_BROKERS,
            'group.id': f'test-consumer-{uuid4()}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'enable.partition.eof': True
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([RECOMMENDATIONS_TOPIC])
        
        # Ждем assignment
        assignment = None
        for _ in range(10):
            assignment = consumer.assignment()
            if assignment:
                break
            consumer.poll(0.1)
        
        logger.info(f"Consumer assignment: {assignment}")
        
        # Если есть assignment, сбрасываем offsets на начало
        if assignment:
            # Устанавливаем offset на 0 для всех партиций
            for tp in assignment:
                tp.offset = 0
            consumer.assign(assignment)
        
        # Читаем все сообщения
        all_messages = []
        matched_messages = []
        start_time = datetime.utcnow()
        
        while len(all_messages) < max_messages:
            msg = consumer.poll(1.0)
            
            if msg is None:
                logger.info("No more messages")
                break
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition {msg.partition()}")
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    break
            
            # Обрабатываем сообщение
            message_info = {
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode('utf-8') if msg.key() else None,
                "value_length": len(msg.value()) if msg.value() else 0,
                "timestamp": msg.timestamp()[1] if msg.timestamp()[0] != 0 else None
            }
            
            all_messages.append(message_info)
            
            # Проверяем ключ
            if msg.key():
                try:
                    key = msg.key().decode('utf-8') if isinstance(msg.key(), bytes) else str(msg.key())
                    if key == client_id:
                        value = msg.value().decode('utf-8') if msg.value() else None
                        matched_messages.append({
                            **message_info,
                            "value": value
                        })
                        logger.info(f"Found matching message for client {client_id} at offset {msg.offset()}")
                except Exception as e:
                    logger.error(f"Error decoding message: {e}")
        
        consumer.close()
        
        # Формируем ответ
        return {
            "status": "success",
            "client_id": client_id,
            "timestamp": datetime.utcnow().isoformat(),
            "search_time_ms": (datetime.utcnow() - start_time).total_seconds() * 1000,
            "total_messages_found": len(all_messages),
            "matched_messages": len(matched_messages),
            "all_messages_sample": all_messages[:10],  # Первые 10 сообщений
            "matched_messages_details": matched_messages,
            "debug_info": {
                "kafka_brokers": RECOMMENDATION_BROKERS,
                "topic": RECOMMENDATIONS_TOPIC,
                "consumer_group": consumer_conf['group.id']
            }
        }
        
    except Exception as e:
        logger.error(f"Test recommendations error: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/client_info")
async def get_client_info(request: Request):
    client_id = request.state.client_id
    
    if client_id not in client_sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    return {
        "status": "success",
        "client_id": client_id,
        "session_info": client_sessions[client_id]
    }

@app.post("/search")
async def client_search(search_request: SearchRequest, request: Request):
    client_id = search_request.client_id or request.state.client_id
    client_id = create_client_session(client_id)
    
    if client_id in client_sessions:
        client_sessions[client_id]["search_count"] += 1
        client_sessions[client_id]["last_activity"] = datetime.utcnow().isoformat()
    
    logger.info(f"Search request from client {client_id}: words={search_request.search_words}")
    
    # Валидация
    if not client_id or not client_id.strip():
        raise HTTPException(status_code=400, detail="client_id is required")
    
    filtered_words = [word.strip() for word in search_request.search_words if word and word.strip()]
    if not filtered_words:
        raise HTTPException(status_code=400, detail="search_words must contain at least one non-empty word")
    
    # Подготовка Avro сообщения
    message_data = {
        'client_id': client_id.strip(),
        'search_words': filtered_words,
        'timestamp': int(datetime.utcnow().timestamp() * 1000)
    }
    
    try:
        # Сериализация и отправка
        serialized_value = avro_serializer(
            message_data, 
            SerializationContext(CLIENT_SEARCHES_TOPIC, MessageField.VALUE)
        )
        
        search_producer.produce(
            topic=CLIENT_SEARCHES_TOPIC,
            key=client_id,
            value=serialized_value,
            callback=delivery_report
        )
        
        search_producer.poll(0)
        
        logger.info(f"Search sent to Kafka: client={client_id}, words={filtered_words}")
        
        response = {
            "status": "success",
            "message": "Search request sent to Kafka in Avro format",
            "data": {
                "client_id": message_data['client_id'],
                "search_words": message_data['search_words'],
                "timestamp": message_data['timestamp'],
                "timestamp_iso": datetime.fromtimestamp(message_data['timestamp'] / 1000).isoformat()
            }
        }
        
        return JSONResponse(
            content=response,
            headers={"X-Client-ID": client_id}
        )
        
    except Exception as e:
        logger.error(f"Failed to send message: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Message delivery failed: {str(e)}")

@app.get("/recommendations")
async def get_recommendations(
    request: Request,
    client_id: Optional[str] = None,
    max_messages: int = Query(10, ge=1, le=100),
    timeout_ms: int = Query(3000, ge=100, le=30000),
    read_all: bool = Query(False)
):
    """Получение рекомендаций для клиента"""
    
    target_client_id = client_id or request.state.client_id
    if not target_client_id:
        target_client_id = create_client_session()
    
    if target_client_id in client_sessions:
        client_sessions[target_client_id]["recommendation_requests"] += 1
        client_sessions[target_client_id]["last_activity"] = datetime.utcnow().isoformat()
    
    logger.info(f"Getting recommendations for client: {target_client_id}, read_all={read_all}")
    
    try:
        consumer = create_recommendation_consumer(read_from_beginning=read_all)
        
        recommendations = []
        messages_checked = 0
        start_time = datetime.utcnow()
        timeout_seconds = timeout_ms / 1000.0
        
        try:
            # Если read_all, сбрасываем offset на начало
            if read_all:
                assignment = consumer.assignment()
                if assignment:
                    for tp in assignment:
                        tp.offset = 0
                    consumer.assign(assignment)
            
            while len(recommendations) < max_messages:
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                if elapsed >= timeout_seconds:
                    logger.info(f"Timeout reached")
                    break
                
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"Reached end of partition")
                        if not read_all:
                            break
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                messages_checked += 1
                
                # Обрабатываем ключ
                if msg.key():
                    try:
                        message_key = msg.key().decode('utf-8') if isinstance(msg.key(), bytes) else str(msg.key())
                        
                        if message_key == target_client_id:
                            # Декодируем значение
                            if msg.value():
                                try:
                                    message_data = json.loads(msg.value().decode('utf-8'))
                                except:
                                    message_data = {"raw": msg.value().decode('utf-8')}
                            else:
                                message_data = None
                            
                            recommendations.append({
                                "data": message_data,
                                "kafka_metadata": {
                                    "topic": msg.topic(),
                                    "partition": msg.partition(),
                                    "offset": msg.offset(),
                                    "timestamp": msg.timestamp()[1] if msg.timestamp()[0] != 0 else None,
                                    "key": message_key
                                }
                            })
                            
                            logger.info(f"Found recommendation at offset {msg.offset()}")
                            
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        continue
                else:
                    logger.debug(f"Message without key at offset {msg.offset()}")
        
        finally:
            consumer.close()
        
        response_data = {
            "status": "success" if recommendations else "no_data",
            "client_id": target_client_id,
            "timestamp": datetime.utcnow().isoformat(),
            "search_params": {
                "read_all": read_all,
                "max_messages": max_messages,
                "timeout_ms": timeout_ms,
                "actual_time_ms": (datetime.utcnow() - start_time).total_seconds() * 1000,
                "messages_checked": messages_checked
            },
            "results": {
                "messages_found": len(recommendations),
                "recommendations": recommendations
            }
        }
        
        return JSONResponse(
            content=response_data,
            headers={"X-Client-ID": target_client_id}
        )
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get recommendations: {str(e)}")

@app.post("/recommendations")
async def get_recommendations_post(
    request: Request,
    rec_request: RecommendationRequest,
    max_messages: int = Query(10, ge=1, le=100),
    timeout_ms: int = Query(3000, ge=100, le=30000),
    read_all: bool = Query(False)
):
    target_client_id = rec_request.client_id or request.state.client_id
    return await get_recommendations(
        request, 
        client_id=target_client_id,
        max_messages=max_messages,
        timeout_ms=timeout_ms,
        read_all=read_all
    )

@app.post("/test/send_recommendation")
async def test_send_recommendation(
    client_id: str = Query("123"),
    message: str = Query('{"test": "recommendation", "score": 0.95}')
):
    """Endpoint для тестовой отправки рекомендаций в Kafka"""
    try:
        from confluent_kafka import Producer
        
        producer = Producer({'bootstrap.servers': RECOMMENDATION_BROKERS})
        
        producer.produce(
            topic=RECOMMENDATIONS_TOPIC,
            key=client_id,
            value=message.encode('utf-8')
        )
        producer.flush()
        
        logger.info(f"Test recommendation sent for client {client_id}: {message}")
        
        return {
            "status": "success",
            "message": f"Test recommendation sent for client {client_id}",
            "data": json.loads(message)
        }
        
    except Exception as e:
        logger.error(f"Test send error: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=6078,
        log_level="info"
    )