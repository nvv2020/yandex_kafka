FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py message.py consumer.py ./

COPY ./kafka-client/ /app/kafka-client/

ENV KAFKA_MODE=docker-ssl
ENV PYTHONUNBUFFERED=1

CMD ["python", "producer.py"]