# Практическая работа 2: "Система обработки сообщений с блокировкой и цензурой"

## Назначение

Система реализует потоковую обработку сообщений с функциями:

- **Блокировка пользователей** - пользователи могут блокировать других пользователей
- **Цензура сообщений** - автоматическое фильтрование запрещенных слов

## Топики Kafka

- `messages` - входящие сообщения
- `blocked-users` - действия блокировки/разблокировки
- `filtered-messages` - обработанные сообщения
- `ban-word-updates` - запрещенные сообщения

## Модель данных

### Message

```json
{
  "message_id": "string",
  "sender": "string",
  "receiver": "string",
  "content": "string",
  "timestamp": "string"
}
```

### BlockedUser

```json
{
  "user_id": "string",
  "blocked_user_id": "string",
  "action": "block|unblock"
}
```

### FilteredMessage

```json
{
  "message_id": "string",
  "sender": "string",
  "receiver": "string",
  "original_content": "string",
  "filtered_content": "string",
  "was_blocked": "boolean",
  "was_filtered": "boolean",
  "timestamp": "string"
}
```

### BanWordsTable

```json
{
  "action": "string",
  "word": "string"
}
```

## Логика работы

### Блокировка пользователей

Каждый пользователь имеет свой список блокировок.
Сообщения от заблокированных пользователей не доставляются адресату.
Списки хранятся в Faust Table.

### Цензура сообщений

Есть глобальный список запрещенных слов.
Запрещенные слова заменяются на звездочки.
Список запрещенных слов можно редактировать (добавлять/удалять слова).

### Требования

Docker
Docker Compose

## Инструкция по запуску

1. Сохраните все файлы в одной директории
2. Выполните: `docker-compose --profile setup up -d`. При поднятии сервисов автоматически создадутся топики messages, blocked-users, filtered-messages, ban-word-updates.

## Тестирование

1. В Kafka UI печатаем обычное сообщения в топик messages:

    ```json
    {
    "message_id": "1",
    "sender": "user1",
    "receiver": "user2",
    "content": "Привет, как дела?",
    "timestamp": "2025-10-14 10:00:00"
    }
    ```

    Результат: сообщение напечатно в топике filtered-messages.

2. Добавляем слово в список запрещенных слов

  ```bash
  curl http://localhost:6066/banwords/
  curl -X POST http://localhost:6066/banwords/ \
  -H "Content-Type: application/json" \
  -d '{"word":"плохоеслово"}'
  ```

  Проверяем, что оно появилось в топике ban-word-updates. В логе `Добавлено запрещенное слово: плохоеслово`.
  Печатаем сообщение с запрещенным словом в топик messages:

  ```json
  {
  "message_id": "2",
  "sender": "user1",
  "receiver": "user2",
  "content": "плохоеслово",
  "timestamp": "2025-10-14 10:01:00"
  }
  ```

  Результат: сообщение напечатано в топике filtered-messages:

  ```json
  {
    "message_id": "2",
    "sender": "user1",
    "receiver": "user2",
    "original_content": "плохоеслово",
    "filtered_content": "***********",
    "was_blocked": false,
    "was_filtered": true,
    "timestamp": "2025-10-14 10:01:00",
    "__faust": {
      "ns": "app.FilteredMessage"
    }
  }
  ```

3. Выполняем блокировку пользователя: печатаем в топик blocked-users сообщение:

  ```json
  {
  "user_id": "user2",
  "blocked_user_id": "user1",
  "action": "block"
  }
    ```

  Печатаем сообщение в топик messages

  ```json
  {
  "message_id": "1",
  "sender": "user1",
  "receiver": "user2",
  "content": "I blocked you.",
  "timestamp": "2025-10-14 10:00:00"
  }
  ```

  Сообщение не попадает в топик filtered-messages. В логах message-processor:

  ```bash
  [2025-10-14 17:50:33,977] [1] [WARNING] Сообщение 1 заблокировано:
  [2025-10-14 17:50:33,978] [1] [WARNING] user1 -> user2
  ```

4. Выполняем разблокировку пользователя: печатаем в топик blocked-users сообщение:

  ```json
  {
  "user_id": "user2",
  "blocked_user_id": "user1",
  "action": "unblock"
  }
  ```

  Ранее заблокированное сообщение проходит в топик filtered-messages.


## API для управления запрещенными словами

```bash
# внести запрещенное слово в список
POST /banwords/<banword>
# удалить запрещенное слово из списка
DELETE /banwords/<banword>
```

### Проверка работы

- В логах Faust приложения видны логи обработки сообщений `docker logs message-processor`
- В Kafka UI (<http://localhost:8085>) можно отслеживать топики и сообщения в них в соответствие с правилами
