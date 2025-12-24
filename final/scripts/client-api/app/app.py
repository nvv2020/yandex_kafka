import os
import json
import logging
import ssl
from datetime import datetime
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SearchRequest(BaseModel):
    id_client: str
    word_search: str

class KafkaMessage(BaseModel):
    message_id: str
    id_client: str
    word_search: str
    timestamp: str

app = FastAPI(
    title="Client Search API",
    description="Client Search API",
    version="1.0.0"
)

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka-1:9093,kafka-2:9095,kafka-3:9097')
MESSAGES_TOPIC = os.getenv('MESSAGES_TOPIC', 'client_search')
SSL_CA = os.getenv('KAFKA_SSL_CA', '/app/certs/ca.crt')
SSL_CERT = os.getenv('KAFKA_SSL_CERT', '/app/certs/client.crt')
SSL_KEY = os.getenv('KAFKA_SSL_KEY', '/app/certs/client.key')

producer = None

def create_ssl_context():
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

@app.on_event("startup")
async def startup_event():
    global producer
    try:
        # SSL контекст
        ssl_context = create_ssl_context()
        producer_config = {
            'bootstrap_servers': KAFKA_BROKERS.split(','),
            'value_serializer': lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            'key_serializer': lambda v: v.encode('utf-8') if isinstance(v, str) else v,
            'security_protocol': 'SSL',
            'ssl_context': ssl_context,
            'acks': 'all',
            'retries': 3,
        }
        producer = KafkaProducer(**producer_config)
        logger.info(f"Kafka producer created for brokers: {KAFKA_BROKERS}")
        
        # # Тестируем подключение
        # test_future = producer.send('test-connection', {'test': 'message'})
        # test_future.get(timeout=10)
        # logger.info("Kafka connection test successful")
        
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        producer = None
        raise

# @app.on_event("shutdown")
# async def shutdown_event():
#     global producer
#     if producer:
#         try:
#             producer.flush(timeout=5)
#             producer.close(timeout=5)
#             logger.info("Kafka producer closed")
#         except Exception as e:
#             logger.error(f"Error closing producer: {e}")

# @app.get("/")
# async def root():
#     """Корневой endpoint"""
#     return {
#         "service": "simple-client-search-api",
#         "version": "1.0.0",
#         "description": "Простой API для отправки поисковых запросов в Kafka",
#         "endpoint": "POST /search - Отправить поисковый запрос"
#     }

@app.post("/search")
async def search(search_request: SearchRequest):
    if not producer:
        raise HTTPException(status_code=503, detail="Service unavailable - Kafka producer not connected")
    if not search_request.id_client or not search_request.id_client.strip():
        raise HTTPException(status_code=400, detail="id_client is required")
    if not search_request.word_search or not search_request.word_search.strip():
        raise HTTPException(status_code=400, detail="word_search is required")
    message = KafkaMessage(
        message_id=str(uuid4()),
        id_client=search_request.id_client.strip(),
        word_search=search_request.word_search.strip(),
        timestamp=datetime.utcnow().isoformat()
    )
    logger.info(f"Processing search request: client={message.id_client}, search='{message.word_search}'")
    try:
        key = None
        if message.id_client:
            # Преобразуем client_id в bytes для использования как ключ
            if isinstance(message.id_client, str):
                key = message.id_client.encode('utf-8')
            elif isinstance(message.id_client, bytes):
                key = message.id_client
            else:
                key = str(message.id_client).encode('utf-8')
        
        # Отправка в Kafka
        future = producer.send(
            topic=MESSAGES_TOPIC,
            key=key,  # Можно закомментировать, если не нужен ключ
            value=message.model_dump()
        )
        
        # Ждем подтверждения
        record_metadata = future.get(timeout=10)
        
        logger.info(f"Message sent to Kafka. Topic: {record_metadata.topic}, "
                   f"Partition: {record_metadata.partition}, "
                   f"Offset: {record_metadata.offset}")
        
        response = {
            "status": "success",
            "message": "Search request sent to Kafka",
            "data": {
                "request_id": message.message_id,
                "client_id": message.id_client,
                "word_search": message.word_search,
                "timestamp": message.timestamp
            },
            "kafka_info": {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset
            }
        }
        
        return JSONResponse(content=response, status_code=200)
        
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send message to Kafka: {str(e)}")
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# @app.get("/health")
# async def health_check():
#     status = {
#         "status": "healthy" if producer else "unhealthy",
#         "timestamp": datetime.utcnow().isoformat(),
#         "kafka_connected": producer is not None,
#         "topic": MESSAGES_TOPIC
#     }
    
#     return JSONResponse(content=status)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=6078,
        log_level="info",
        reload=True
    )