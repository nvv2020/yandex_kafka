# Настройка защищённого соединения и управление доступом.

## Порядок работы:

```bash
# ставим OpenSSL
apt install openssl

# cоздаем файл ca/ca.cnf

# создаем новый сертификатный запрос без шифрования приватного ключа
openssl req -new -nodes -x509 -days 365 -newkey rsa:2048 -keyout ca.key -out ca.crt -config ca.cnf

# Объединим сертификат CA (ca.crt) и его ключ (ca.key) в один файл ca.pem
cat ca.crt ca.key > ca.pem

# создаем файлы конфигурации kafka-xxx-creds/kafka-xxx.cnf

# создаем приватные ключи и запросы на сертификат (CSR)
openssl req -new \
    -newkey rsa:2048 \
    -keyout kafka-1-creds/kafka-1.key \
    -out kafka-1-creds/kafka-1.csr \
    -config kafka-1-creds/kafka-1.cnf \
    -nodes

openssl req -new \
    -newkey rsa:2048 \
    -keyout kafka-2-creds/kafka-2.key \
    -out kafka-2-creds/kafka-2.csr \
    -config kafka-2-creds/kafka-2.cnf \
    -nodes

openssl req -new \
    -newkey rsa:2048 \
    -keyout kafka-3-creds/kafka-3.key \
    -out kafka-3-creds/kafka-3.csr \
    -config kafka-3-creds/kafka-3.cnf \
    -nodes

# создаем сертификаты, подписанные CA
openssl x509 -req \
    -days 3650 \
    -in kafka-1-creds/kafka-1.csr \
    -CA ca/ca.crt \
    -CAkey ca/ca.key \
    -CAcreateserial \
    -out kafka-1-creds/kafka-1.crt \
    -extfile kafka-1-creds/kafka-1.cnf \
    -extensions v3_req

openssl x509 -req \
    -days 3650 \
    -in kafka-2-creds/kafka-2.csr \
    -CA ca/ca.crt \
    -CAkey ca/ca.key \
    -CAcreateserial \
    -out kafka-2-creds/kafka-2.crt \
    -extfile kafka-2-creds/kafka-2.cnf \
    -extensions v3_req

openssl x509 -req \
    -days 3650 \
    -in kafka-3-creds/kafka-3.csr \
    -CA ca/ca.crt \
    -CAkey ca/ca.key \
    -CAcreateserial \
    -out kafka-3-creds/kafka-3.crt \
    -extfile kafka-3-creds/kafka-3.cnf \
    -extensions v3_req

# создаем PKCS12-хранилище
openssl pkcs12 -export \
    -in kafka-1-creds/kafka-1.crt \
    -inkey kafka-1-creds/kafka-1.key \
    -chain \
    -CAfile ca/ca.pem \
    -name kafka-1 \
    -out kafka-1-creds/kafka-1.p12 \
    -password pass:my-password

openssl pkcs12 -export \
    -in kafka-2-creds/kafka-2.crt \
    -inkey kafka-2-creds/kafka-2.key \
    -chain \
    -CAfile ca/ca.pem \
    -name kafka-2 \
    -out kafka-2-creds/kafka-2.p12 \
    -password pass:my-password

openssl pkcs12 -export \
    -in kafka-3-creds/kafka-3.crt \
    -inkey kafka-3-creds/kafka-3.key \
    -chain \
    -CAfile ca/ca.pem \
    -name kafka-3 \
    -out kafka-3-creds/kafka-3.p12 \
    -password pass:my-password

# создаем keystore для Kafka
keytool -importkeystore \
    -deststorepass my-password \
    -destkeystore kafka-1-creds/kafka.kafka-1.keystore.pkcs12 \
    -srckeystore kafka-1-creds/kafka-1.p12 \
    -deststoretype PKCS12  \
    -srcstoretype PKCS12 \
    -noprompt \
    -srcstorepass my-password

keytool -importkeystore \
    -deststorepass my-password \
    -destkeystore kafka-2-creds/kafka.kafka-2.keystore.pkcs12 \
    -srckeystore kafka-2-creds/kafka-2.p12 \
    -deststoretype PKCS12  \
    -srcstoretype PKCS12 \
    -noprompt \
    -srcstorepass my-password

keytool -importkeystore \
    -deststorepass my-password \
    -destkeystore kafka-3-creds/kafka.kafka-3.keystore.pkcs12 \
    -srckeystore kafka-3-creds/kafka-3.p12 \
    -deststoretype PKCS12  \
    -srcstoretype PKCS12 \
    -noprompt \
    -srcstorepass my-password

# создаем truststore для Kafka
keytool -import \
    -file ca/ca.crt \
    -alias ca \
    -keystore kafka-1-creds/kafka.kafka-1.truststore.jks \
    -storepass my-password \
    -noprompt

keytool -import \
    -file ca/ca.crt \
    -alias ca \
    -keystore kafka-2-creds/kafka.kafka-2.truststore.jks \
    -storepass my-password \
    -noprompt

keytool -import \
    -file ca/ca.crt \
    -alias ca \
    -keystore kafka-3-creds/kafka.kafka-3.truststore.jks \
    -storepass my-password \
    -noprompt

# Сохраним пароли
echo "my-password" > kafka-1-creds/kafka-1_sslkey_creds
echo "my-password" > kafka-1-creds/kafka-1_keystore_creds
echo "my-password" > kafka-1-creds/kafka-1_truststore_creds

echo "my-password" > kafka-2-creds/kafka-2_sslkey_creds
echo "my-password" > kafka-2-creds/kafka-2_keystore_creds
echo "my-password" > kafka-2-creds/kafka-2_truststore_creds

echo "my-password" > kafka-3-creds/kafka-3_sslkey_creds
echo "my-password" > kafka-3-creds/kafka-3_keystore_creds
echo "my-password" > kafka-3-creds/kafka-3_truststore_creds

# для клиента
# 1. Генерация ключа и CSR
openssl req -new \
    -newkey rsa:2048 \
    -keyout kafka-client/client.key \
    -out kafka-client/client.csr \
    -subj "/CN=kafka-client" \
    -nodes

# 2. Подписание сертификата
openssl x509 -req \
    -days 365 \
    -in kafka-client/client.csr \
    -CA ca/ca.crt \
    -CAkey ca/ca.key \
    -out kafka-client/client.crt

# создадим docker-compose.yml c конфигурацией для Zookeeper и трёх брокеров Kafka, поднимем кластер
docker compose up -d zookeeper kafka-1 kafka-2 kafka-3

#  проверим, что всё работает корректно
docker exec -it kafka-1 kafka-cluster cluster-id --bootstrap-server kafka-1:9093 --config /etc/kafka/secrets/client-ssl.properties
>> Cluster ID: ...
netstat -an | grep -E '9093|9095|9097'
>> tcp        0      0 0.0.0.0:9097            0.0.0.0:*               LISTEN
>> tcp        0      0 0.0.0.0:9095            0.0.0.0:*               LISTEN
>> tcp        0      0 0.0.0.0:9093            0.0.0.0:*               LISTEN
docker exec -it kafka-1 openssl s_client -connect localhost:9093 -CAfile /etc/kafka/secrets/ca.crt
>> Verification: OK

# создадим два топика: topic-1 и topic-2
docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:19092 \
  --create \
  --topic topic-1 \
  --partitions 3 \
  --replication-factor 3

docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:19092 \
  --create \
  --topic topic-2 \
  --partitions 3 \
  --replication-factor 3

# проверим, что создались топики
docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:19092 --list
>> topic-1
>> topic-2

# Настроим права доступа к топикам
# topic-1: Доступен как для продюсеров, так и для консьюмеров.
# topic-2: Продюсеры могут отправлять сообщения. Консьюмеры не имеют доступа к чтению данных.

docker exec -it kafka-1 kafka-acls \
  --add \
  --allow-principal User:CN=kafka-client \
  --operation WRITE \
  --operation DESCRIBE \
  --topic topic-1 \
  --bootstrap-server kafka-1:19092

docker exec -it kafka-1 kafka-acls \
  --add \
  --allow-principal User:CN=kafka-client \
  --operation WRITE \
  --operation DESCRIBE \
  --topic topic-2 \
  --bootstrap-server kafka-1:19092

docker exec -it kafka-1 kafka-acls \
  --add \
  --allow-principal User:CN=kafka-client \
  --operation READ \
  --operation DESCRIBE \
  --topic topic-1 \
  --bootstrap-server kafka-1:19092

docker exec -it kafka-1 kafka-acls --bootstrap-server kafka-1:19092 --list
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=topic-1, patternType=LITERAL)`:
        (principal=User:CN=kafka-client, host=*, operation=DESCRIBE, permissionType=ALLOW)
        (principal=User:CN=kafka-client, host=*, operation=WRITE, permissionType=ALLOW)
        (principal=User:CN=kafka-client, host=*, operation=READ, permissionType=ALLOW)

Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=topic-2, patternType=LITERAL)`:
        (principal=User:CN=kafka-client, host=*, operation=WRITE, permissionType=ALLOW)
        (principal=User:CN=kafka-client, host=*, operation=DESCRIBE, permissionType=ALLOW)

# запустим продьюсер
docker compose build producer
docker compose up --no-deps producer -d
docker logs -f kafka-cluster-producer-1

# проверим, что сообщения появляются: подпишемся на сообщения для проверки публикации сообщений в топиках
docker exec -it kafka-1 kafka-console-consumer --bootstrap-server localhost:19092 --topic topic-1 --from-beginning
docker exec -it kafka-1 kafka-console-consumer --bootstrap-server localhost:19092 --topic topic-2 --from-beginning

# запускаем консьюмер
docker compose build consumer
docker compose up --no-deps consumer -d
docker logs -f consumer
docker logs -f consumer

# в выводе видим, что читать смог только из topic-1
Starting Kafka consumers for topics: topic-1, topic-2
✓ Consumer created for topic-1
✓ Consumer created for topic-2
Starting parallel consumption from 2 topic(s)
Starting consumption from topic-1
Starting consumption from topic-2
Error in consumer for topic-2: [Error 29] TopicAuthorizationFailedError: {'i', 'p', '2', '-', 'c', 'o', 't'}
Consumer for topic-2 closed
[topic-1] key-cb189f18-a8e5-45a3-af13-92946ffc958b: SSL test message at 2025-12-02 10:21:01.305376
[topic-1] key-032ae7c3-bf46-4c91-9d2f-05b6c8b9586f: SSL test message at 2025-12-02 10:21:37.906017
[topic-1] key-17ae67d5-d206-4b00-b101-a1f0facaee25: SSL test message at 2025-12-02 10:21:40.920470

```

