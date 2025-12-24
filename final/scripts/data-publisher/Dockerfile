# FROM python:3.12-slim

# WORKDIR /app

# # RUN pip install --no-cache-dir \
# #     confluent-kafka \
# #     avro \
# #     avro-python3 \
# #     attrs \
# #     certifi \
# #     httpx \
# #     cachetools \
# #     authlib \
# #     fastavro \
# #     python-dateutil

# COPY app.py .

# CMD ["python", "app.py"]

FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# COPY schema/ ./schema/
COPY app.py .
CMD ["python", "-u", "app.py"]