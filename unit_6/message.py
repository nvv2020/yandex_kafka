import json
import avro.schema
import avro.io
import io
from datetime import datetime
from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)

class Message:
    def __init__(self, id: int, content: str, timestamp: datetime = None):
        self.id = id
        self.content = content
        self.timestamp = timestamp or datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'content': self.content,
            'timestamp': self.timestamp.isoformat()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        return cls(
            id=data['id'],
            content=data['content'],
            timestamp=datetime.fromisoformat(data['timestamp'])
        )

    def __str__(self):
        return f"Message(id={self.id}, content='{self.content}', timestamp={self.timestamp})"

class MessageSerializer:
    @staticmethod
    def serialize_json(message: Message) -> bytes:
        """Сериализация сообщения в JSON"""
        try:
            return json.dumps(message.to_dict()).encode('utf-8')
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise

    @staticmethod
    def deserialize_json(data: bytes) -> Message:
        """Десериализация сообщения из JSON"""
        try:
            message_dict = json.loads(data.decode('utf-8'))
            return Message.from_dict(message_dict)
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise

class MessageProcessor:
    @staticmethod
    def process_message(message: Message) -> bool:
        """
        Обработка сообщения
        Возвращает True если обработка успешна, False в случае ошибки
        """
        try:
            # Имитация обработки сообщения
            print(f"Processing: {message}")

            # Имитация возможной ошибки обработки
            if "error" in message.content.lower():
                raise ValueError("Simulated processing error")

            return True
        except Exception as e:
            logger.error(f"Error processing message {message.id}: {e}")
            return False