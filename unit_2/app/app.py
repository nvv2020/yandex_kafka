import asyncio
import faust
from faust.web import Request, Response, View
import json
import logging
import re
import os
from typing import Set, Dict, List


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

broker_urls = os.getenv('KAFKA_BROKERS',
                        'kafka-0:9092,kafka-1:9092,kafka-2:9092')
broker_list = [f'kafka://{broker.strip()}'
               for broker in broker_urls.split(',')]
messages = os.getenv('MESSAGES_TOPIC', 'messages')
blocked_users = os.getenv('BLOCKED_USERS_TOPIC', 'blocked-users')
filtered_messages = os.getenv('FILTERED_MESSAGES_TOPIC', 'filtered-messages')
ban_word_updates = os.getenv('BAN_WORD_UPDATES_TOPIC', 'ban-word-updates')

print(f"Config: broker_list={broker_list}")
print(f"Config: messages={messages}")
print(f"Config: blocked_users={blocked_users}")
print(f"Config: filtered_messages={filtered_messages}")


app = faust.App(
    'message-processor',
    broker=broker_list,
    store='memory://',
    version=1,
    topic_partitions=1,
    # broker_request_timeout=30.0,
    # broker_commit_interval=1.0,
    autodiscover=True,
    origin='app',
    # broker_session_timeout=30.0,
    # broker_heartbeat_interval=10.0,
    broker_api_version='2.8.0',
    consumer_auto_offset_reset='earliest',
    topic_allow_declare=False,
    topic_disable_leader=True,
    web_enabled=True,
    web_port=6066,
)


# Модели данных
class Message(faust.Record):
    message_id: str
    sender: str
    receiver: str
    content: str
    timestamp: str


class BlockedUser(faust.Record):
    user_id: str
    blocked_user_id: str
    action: str  # 'block' or 'unblock'


class FilteredMessage(faust.Record):
    message_id: str
    sender: str
    receiver: str
    original_content: str
    filtered_content: str
    was_blocked: bool
    was_filtered: bool
    timestamp: str


class BanWordsTable(faust.Record):
    words: List[str]


class BanWordUpdate(faust.Record):
    action: str  # 'add' or 'remove'
    word: str


# Топики Kafka
messages_topic = app.topic(messages, value_type=Message)
blocked_users_topic = app.topic(blocked_users, value_type=BlockedUser)
filtered_messages_topic = app.topic(filtered_messages,
                                    value_type=FilteredMessage)
ban_word_updates_topic = app.topic(ban_word_updates, value_type=BanWordUpdate)

# Таблица для хранения заблокированных пользователей
blocked_users_table = app.Table(
    'blocked_users_table',
    default=set,
    partitions=1
)

# Таблица для хранения запрещенных слов
banned_words_table = app.Table(
    'banned_words',
    value_type=BanWordsTable,
    default=lambda: BanWordsTable(words=[]),
    partitions=1
)

@app.agent(ban_word_updates_topic)
async def handle_ban_word_updates(stream):
    async for update in stream:
        # Используем ключ 'global' для хранения глобального списка слов
        current_data = banned_words_table.get('global')
        if current_data is None:
            current_words = []
        else:
            current_words = current_data.words

        word = update.word.lower().strip()

        if update.action == 'add':
            if word not in current_words:
                current_words.append(word)
                banned_words_table['global'] = BanWordsTable(words=current_words)
                print(f"Добавлено запрещенное слово: {word}")
                print(f"Текущий список: {current_words}")
        elif update.action == 'remove':
            if word in current_words:
                current_words.remove(word)
                banned_words_table['global'] = BanWordsTable(words=current_words)
                print(f"Удалено запрещенное слово: {word}")
                print(f"Текущий список: {current_words}")


# Обработчик обновлений таблицы заблокированных пользователей
@app.agent(blocked_users_topic)
async def update_blocked_users(stream):
    async for blocked_user in stream:
        user_id = blocked_user.user_id
        blocked_id = blocked_user.blocked_user_id

        if blocked_user.action == 'block':
            if user_id not in blocked_users_table:
                blocked_users_table[user_id] = set()
            blocked_users_table[user_id].add(blocked_id)
            print(f"Пользователь {blocked_id} заблокирован для {user_id}")

        elif blocked_user.action == 'unblock':
            user_blocked_set = blocked_users_table.get(user_id, set())
            if blocked_id in user_blocked_set:
                blocked_users_table[user_id].remove(blocked_id)
                print(f"Пользователь {blocked_id} разблокирован для {user_id}")


# Функция для цензуры текста
def censor_text(text: str, banned_words: Set[str]) -> tuple[str, bool]:
    """
    Заменяет запрещенные слова на звездочки
    Возвращает отфильтрованный текст и флаг был ли текст изменен
    """
    was_filtered = False
    filtered_text = text

    for word in banned_words:
        pattern = re.compile(re.escape(word), re.IGNORECASE)
        if pattern.search(filtered_text):
            filtered_text = pattern.sub('*' * len(word), filtered_text)
            was_filtered = True

    return filtered_text, was_filtered


# Функция проверки блокировки пользователя
def is_user_blocked(
        receiver: str,
        sender: str,
        blocked_table: Dict[str, Set[str]]) -> bool:
    """
    Проверяет, заблокирован ли отправитель для получателя
    """
    if receiver in blocked_table and sender in blocked_table[receiver]:
        return True
    return False


# Основной обработчик сообщений
@app.agent(messages_topic)
async def process_messages(stream):
    async for message in stream:
        # Получаем список запрещенных слов из таблицы
        banned_words_data = banned_words_table.get('global')
        if banned_words_data is None:
            banned_words_set = set()
        else:
            banned_words_set = set(banned_words_data.words)

        # Проверяем блокировку
        is_blocked = is_user_blocked(message.receiver, message.sender,
                                     blocked_users_table)

        # Если сообщение заблокировано
        if is_blocked:
            # Создаем объект FilteredMessage с флагом was_blocked=True
            filtered_message = FilteredMessage(
                message_id=message.message_id,
                sender=message.sender,
                receiver=message.receiver,
                original_content=message.content,
                filtered_content="[BLOCKED]",
                was_blocked=True,
                was_filtered=False,
                timestamp=message.timestamp
            )
            print(f"Сообщение {message.message_id} заблокировано: ")
            print(f"{message.sender} -> {message.receiver}")

        else:
            # Применяем цензуру к сообщению
            filtered_content, was_filtered = censor_text(message.content,
                                                         banned_words_set)

            # Создаем объект FilteredMessage
            filtered_message = FilteredMessage(
                message_id=message.message_id,
                sender=message.sender,
                receiver=message.receiver,
                original_content=message.content,
                filtered_content=filtered_content,
                was_blocked=False,
                was_filtered=was_filtered,
                timestamp=message.timestamp
            )

            # Логируем результат фильтрации
            if was_filtered:
                print(f"Сообщение {message.message_id} отфильтровано: ")
                print(f"{message.content} -> {filtered_content}")
            else:
                print(f"Сообщение {message.message_id} доставлено: ")
                print(f"{message.sender} -> {message.receiver}")

        # Отправляем обработанное сообщение в выходной топик
        await filtered_messages_topic.send(value=filtered_message)


# API методы для работы с запрещенными словами
@app.page('/banwords/{word}')
class BanWordDetailView(View):
    async def delete(self, request: Request, word: str) -> Response:
        """Удалить слово из списка запрещённых"""
        try:
            # Отправляем событие удаления в Kafka
            update = BanWordUpdate(action='remove', word=word)
            await ban_word_updates_topic.send(value=update)

            return self.json({
                'status': 'success',
                'message': f'Слово "{word}" отправлено на удаление',
                'word': word
            })
        except Exception as e:
            return self.json({
                'status': 'error',
                'message': f'Ошибка при удалении слова: {str(e)}'
            }, status=500)


@app.page('/banwords/')
class BanWordsView(View):
    async def get(self, request: Request) -> Response:
        """Получить текущий список запрещённых слов"""
        try:
            # Получаем данные по ключу 'global'
            banned_words_data = banned_words_table.get('global')
            if banned_words_data is None:
                words = []
            else:
                words = banned_words_data.words

            print(f"Текущие запрещенные слова: {words}")  # Для отладки

            return self.json({
                'status': 'success',
                'banned_words': words,
                'count': len(words)
            })
        except Exception as e:
            print(f"Ошибка при получении списка слов: {str(e)}")  # Для отладки
            return self.json({
                'status': 'error',
                'message': f'Ошибка при получении списка слов: {str(e)}'
            }, status=500)

    async def post(self, request: Request) -> Response:
        """Добавить новое запрещённое слово"""
        try:
            data = await request.json()
            word = data.get('word', '').strip()

            if not word:
                return self.json({
                    'status': 'error',
                    'message': 'Параметр "word" обязателен'
                }, status=400)

            # Отправляем событие добавления в Kafka
            update = BanWordUpdate(action='add', word=word)
            await ban_word_updates_topic.send(value=update)

            return self.json({
                'status': 'success',
                'message': f'Слово "{word}" отправлено на добавление',
                'word': word
            })
        except Exception as e:
            return self.json({
                'status': 'error',
                'message': f'Ошибка при добавлении слова: {str(e)}'
            }, status=500)


if __name__ == '__main__':
    app.main()