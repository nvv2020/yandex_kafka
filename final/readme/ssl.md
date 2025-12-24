# создаем новый сертификатный запрос без шифрования приватного ключа
openssl req -new -nodes -x509 -days 365 -newkey rsa:2048 -keyout ca.key -out ca.crt -config ca.cnf

# Объединим сертификат CA (ca.crt) и его ключ (ca.key) в один файл ca.pem
cat ca.crt ca.key > ca.pem

# создаем файлы конфигурации kafka-xxx-creds/kafka-xxx.cnf

```bash
# создаем приватные ключи и запросы на сертификат (CSR)
openssl req -new \
    -newkey rsa:2048 \
    -keyout certs/kafka-0-creds/kafka-0.key \
    -out certs/kafka-0-creds/kafka-0.csr \
    -config certs/kafka-0-creds/kafka-0.cnf \
    -nodes	

# создаем сертификаты, подписанные CA
openssl x509 -req \
    -days 3650 \
    -in certs/kafka-0-creds/kafka-0.csr \
    -CA certs/ca/ca.crt \
    -CAkey certs/ca/ca.key \
    -CAcreateserial \
    -out certs/kafka-0-creds/kafka-0.crt \
    -extfile certs/kafka-0-creds/kafka-0.cnf \
    -extensions v3_req	

# создаем PKCS12-хранилище
openssl pkcs12 -export \
    -in certs/kafka-0-creds/kafka-0.crt \
    -inkey certs/kafka-0-creds/kafka-0.key \
    -chain \
    -CAfile certs/ca/ca.pem \
    -name kafka-0 \
    -out certs/kafka-0-creds/kafka-0.p12 \
    -password pass:my-password	

# создаем keystore для Kafka
keytool -importkeystore \
    -deststorepass my-password \
    -destkeystore certs/kafka-0-creds/kafka.kafka-0.keystore.pkcs12 \
    -srckeystore certs/kafka-0-creds/kafka-0.p12 \
    -deststoretype PKCS12  \
    -srcstoretype PKCS12 \
    -noprompt \
    -srcstorepass my-password	

# создаем truststore для Kafka
keytool -import \
    -file certs/ca/ca.crt \
    -alias ca \
    -keystore certs/kafka-0-creds/kafka.kafka-0.truststore.jks \
    -storepass my-password \
    -noprompt	
	
# Сохраняемм пароли
echo "my-password" > certs/kafka-0-creds/kafka-0_sslkey_creds
echo "my-password" > certs/kafka-0-creds/kafka-0_keystore_creds
echo "my-password" > certs/kafka-0-creds/kafka-0_truststore_creds
```

