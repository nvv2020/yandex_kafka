API Endpoints:

GET / - проверка работы сервиса
GET /search - основной endpoint для приема запросов
POST /search - альтернативный endpoint через POST

Формат сообщения в Kafka:

```json
{
  "message_id": "uuid",
  "id_client": "client123",
  "word_search": "search query",
  "timestamp": "2024-01-01T00:00:00"
}
```

Использование:

```bash
# GET запрос
curl "http://localhost:6078/search?id_client=client123&word_search=test+query"
# POST запрос
curl -X POST "http://localhost:6078/search" \
     -H "Content-Type: application/json" \
     -d '{"id_client": "client123", "word_search": "test query"}'
```

*************** 

# Первый запрос (создаст сессию автоматически)
curl -v "http://localhost:6078/search?word_search=ноутбук"
# В ответе будет заголовок: X-Client-ID: client_a1b2c3d4

# Последующие запросы с client_id из заголовка или параметра
curl -H "X-Client-ID: client_a1b2c3d4" "http://localhost:6078/search?word_search=телефон"

# Или через параметр
curl "http://localhost:6078/search?word_search=телефон&client_id=client_a1b2c3d4"

# Получение рекомендаций для текущего клиента
curl -H "X-Client-ID: client_a1b2c3d4" "http://localhost:6078/get_recommendation"