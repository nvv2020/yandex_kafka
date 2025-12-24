import asyncio
from faust import App, Record
from faust.web import Request, Response, View
import json
import logging
import os
import ssl
import uuid
import re
from typing import List, Optional, Dict, Any
from datetime import datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройки подключения
broker_urls = os.getenv('KAFKA_BROKERS', 'kafka-1:9093,kafka-2:9095,kafka-3:9097')
broker_list = [f'kafka://{broker.strip()}' for broker in broker_urls.split(',')]

# SSL настройки
ssl_ca = os.getenv('KAFKA_SSL_CA', '/app/certs/ca.crt')
ssl_cert = os.getenv('KAFKA_SSL_CERT', '/app/certs/client.crt')
ssl_key = os.getenv('KAFKA_SSL_KEY', '/app/certs/client.key')

print(f"Config: broker_list={broker_list}")
print(f"SSL Config: ca={ssl_ca}, cert={ssl_cert}, key={ssl_key}")

# Настройки топиков
shop_raw_topic_name = os.getenv('SHOP_RAW_TOPIC', 'shop-raw')
shop_clear_topic_name = os.getenv('SHOP_CLEAR_TOPIC', 'shop-clear')
ban_words_topic_name = os.getenv('BAN_WORDS_TOPIC', 'ban-words')

# Создаем SSL контекст
def create_ssl_context():
    """Создание SSL контекста для подключения к Kafka"""
    context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=ssl_ca
    )
    # Загружаем клиентские сертификаты
    context.load_cert_chain(
        certfile=ssl_cert,
        keyfile=ssl_key
    )
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    return context

# Создаем приложение Faust с SSL
app = App(
    'shop-messages-filter',
    broker=broker_list,
    broker_credentials=create_ssl_context(),
    broker_security_protocol='SSL',
    store='memory://',
    version=1,
    web_port=6068,
    web_enabled=True,
    web_bind='0.0.0.0',
    topic_partitions=1,
    broker_api_version='2.8.0',
    consumer_auto_offset_reset='earliest',
    topic_allow_declare=False,
    topic_disable_leader=True,
    autodiscover=False,
)

# Модели данных
class BanWordUpdate(Record):
    """Событие обновления списка запрещенных слов"""
    action: str  # 'ADD' или 'REMOVE'
    word: str
    timestamp: str
    source: str
    request_id: Optional[str] = None

class ShopMessage(Record):
    """Сообщение с информацией о продукте из shop-raw"""
    product_id: str
    name: str
    description: str
    price: Dict
    category: str
    brand: str
    stock: Dict
    sku: str
    tags: List[str]
    images: List[Dict]
    specifications: Dict
    created_at: str
    updated_at: str
    index: str
    store_id: str

class FilteredShopMessage(Record):
    """Отфильтрованное сообщение для shop_clear"""
    product_id: str
    name: str
    original_description: str
    filtered_description: str
    price: Dict
    category: str
    brand: str
    stock: Dict
    sku: str
    tags: List[str]
    images: List[Dict]
    specifications: Dict
    created_at: str
    updated_at: str
    index: str
    store_id: str
    has_profanity: bool
    profanity_count: int
    banned_words_found: List[str]
    filter_timestamp: str

# Топики Kafka
shop_raw_topic = app.topic(shop_raw_topic_name, value_type=ShopMessage)
shop_clear_topic = app.topic(shop_clear_topic_name, value_type=FilteredShopMessage)
ban_words_topic = app.topic(ban_words_topic_name, value_type=BanWordUpdate)

# Таблица для хранения запрещенных слов
banned_words_table = app.Table(
    'banned_words_table',
    default=lambda: {},
    partitions=1
)

# Статистика обработки
processing_stats = app.Table(
    'processing_stats',
    default=lambda: {'total': 0, 'filtered': 0, 'clean': 0},
    partitions=1
)

# Глобальная переменная для хранения запрещенных слов в памяти
# Используется только для чтения из API
banned_words_cache = {}

@app.task
async def on_started():
    """Задача, выполняемая при старте приложения"""
    logger.info(f"🚀 Shop Messages Filter запущен на порту 6068")
    logger.info(f"🔗 Подключение к Kafka: {broker_list}")
    logger.info(f"📥 Входной топик: {shop_raw_topic_name}")
    logger.info(f"📤 Выходной топик: {shop_clear_topic_name}")
    logger.info(f"🚫 Топик запрещенных слов: {ban_words_topic_name}")
    
    # Инициализируем кэш
    await update_banned_words_cache()
    logger.info(f"📊 Начальное количество запрещенных слов: {len(banned_words_cache)}")

async def update_banned_words_cache():
    """Обновляет кэш запрещенных слов из таблицы"""
    global banned_words_cache
    banned_words_cache = dict(banned_words_table)
    logger.info(f"🔄 Кэш запрещенных слов обновлен: {len(banned_words_cache)} слов")

@app.agent(ban_words_topic)
async def handle_ban_words_updates(stream):
    """
    Обработчик обновлений списка запрещенных слов из Kafka
    """
    async for update in stream:
        try:
            word = update.word.lower().strip()
            logger.info(f"📨 Обновление запрещенных слов из Kafka: {update.action} '{word}' (источник: {update.source})")
            
            if update.action == 'ADD':
                if word not in banned_words_table:
                    banned_words_table[word] = {
                        'word': word,
                        'added_at': update.timestamp,
                        'source': update.source,
                        'request_id': update.request_id
                    }
                    logger.info(f"✅ Добавлено запрещенное слово: {word}")
                else:
                    logger.info(f"ℹ️ Слово '{word}' уже в списке")
                    
            elif update.action == 'REMOVE':
                if word in banned_words_table:
                    banned_words_table.pop(word, None)
                    logger.info(f"✅ Удалено запрещенное слово: {word}")
                else:
                    logger.info(f"ℹ️ Слово '{word}' не найдено в списке")
            
            # Обновляем кэш после изменения таблицы
            await update_banned_words_cache()
                    
            # Логируем текущее количество слов
            word_count = len(banned_words_table)
            logger.info(f"📊 Всего запрещенных слов: {word_count}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки обновления запрещенных слов: {e}", exc_info=True)


def check_text_for_profanity(text: str) -> Dict:
    """
    Проверяет текст на наличие запрещенных слов.
    Возвращает информацию о найденных запрещенных словах.
    """
    # Используем кэш для чтения
    banned_words = list(banned_words_cache.keys())
    
    if not banned_words:
        return {
            'has_profanity': False,
            'profanity_count': 0,
            'banned_words_found': []
        }
    
    banned_words_found = []
    has_profanity = False
    
    # Сортируем слова по длине (от самых длинных к самым коротким)
    # чтобы избежать частичных совпадений
    sorted_banned_words = sorted(banned_words, key=len, reverse=True)
    
    for banned_word in sorted_banned_words:
        # Ищем слово целиком (с границами слов), учитывая регистр
        pattern = rf'\b{re.escape(banned_word)}\b'
        
        # Используем finditer для поиска всех вхождений
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        
        if matches:
            has_profanity = True
            if banned_word not in banned_words_found:
                banned_words_found.append(banned_word)
    
    # Подсчитываем общее количество вхождений
    profanity_count = 0
    if has_profanity:
        for word in banned_words_found:
            pattern = rf'\b{re.escape(word)}\b'
            matches = list(re.finditer(pattern, text, re.IGNORECASE))
            profanity_count += len(matches)
    
    return {
        'has_profanity': has_profanity,
        'profanity_count': profanity_count,
        'banned_words_found': banned_words_found
    }


def check_tags_for_profanity(tags: List[str]) -> Dict:
    """
    Проверяет список тегов на наличие запрещенных слов.
    Возвращает информацию о найденных запрещенных словах.
    """
    # Используем кэш для чтения
    banned_words = list(banned_words_cache.keys())
    
    if not banned_words or not tags:
        return {
            'has_profanity': False,
            'profanity_count': 0,
            'banned_words_found': []
        }
    
    banned_words_found = []
    has_profanity = False
    profanity_count = 0
    
    # Сортируем слова по длине (от самых длинных к самым коротким)
    sorted_banned_words = sorted(banned_words, key=len, reverse=True)
    
    # Проверяем каждый тег
    for tag in tags:
        tag_str = str(tag).lower().strip()
        if not tag_str:
            continue
            
        for banned_word in sorted_banned_words:
            # Проверяем точное совпадение тега с запрещенным словом
            if tag_str == banned_word.lower():
                has_profanity = True
                profanity_count += 1
                if banned_word not in banned_words_found:
                    banned_words_found.append(banned_word)
            else:
                # Также проверяем, содержит ли тег запрещенное слово как часть
                pattern = rf'\b{re.escape(banned_word)}\b'
                matches = list(re.finditer(pattern, tag_str, re.IGNORECASE))
                if matches:
                    has_profanity = True
                    profanity_count += len(matches)
                    if banned_word not in banned_words_found:
                        banned_words_found.append(banned_word)
    
    return {
        'has_profanity': has_profanity,
        'profanity_count': profanity_count,
        'banned_words_found': banned_words_found
    }


def combine_profanity_results(*results: Dict) -> Dict:
    """
    Объединяет результаты нескольких проверок на профанацию.
    """
    combined_result = {
        'has_profanity': False,
        'profanity_count': 0,
        'banned_words_found': []
    }
    
    for result in results:
        if result['has_profanity']:
            combined_result['has_profanity'] = True
            combined_result['profanity_count'] += result['profanity_count']
            
            # Объединяем найденные слова без дубликатов
            for word in result['banned_words_found']:
                if word not in combined_result['banned_words_found']:
                    combined_result['banned_words_found'].append(word)
    
    return combined_result


async def send_ban_word_update(action: str, word: str, source: str = 'api', request_id: str = None):
    """
    Отправляет событие обновления запрещенного слова в Kafka
    """
    try:
        update = BanWordUpdate(
            action=action,
            word=word,
            timestamp=datetime.now().isoformat(),
            source=source,
            request_id=request_id or str(uuid.uuid4())
        )
        
        await ban_words_topic.send(value=update)
        logger.info(f"📤 Отправлено событие в Kafka: {action} '{word}'")
        return update
    except Exception as e:
        logger.error(f"❌ Ошибка отправки события в Kafka: {e}")
        raise


# Основной обработчик сообщений
@app.agent(shop_raw_topic)
async def process_shop_messages(stream):
    """
    Основной обработчик сообщений из shop_raw
    Читает сообщения, фильтрует описание и теги, отправляет в shop_clear только чистые сообщения
    """
    async for message in stream:
        try:
            # Обновляем статистику
            stats = processing_stats.get('global')
            if not stats:
                stats = {'total': 0, 'filtered': 0, 'clean': 0}
            
            stats['total'] += 1
            processing_stats['global'] = stats
            
            logger.info(f"📥 Получено сообщение о продукте: {message.product_id} - {message.name}")
            
            # Получаем текущие запрещенные слова для отладки
            current_banned_words = list(banned_words_cache.keys())
            logger.info(f"🔍 Текущие запрещенные слова ({len(current_banned_words)}): {current_banned_words}")
            
            # Проверяем описание продукта на запрещенные слова
            description_check = check_text_for_profanity(message.description)
            logger.info(f"📝 Проверка описания для {message.product_id}: "
                       f"has_profanity={description_check['has_profanity']}, "
                       f"found={description_check['banned_words_found']}")
            
            # Проверяем теги на запрещенные слова
            tags_check = check_tags_for_profanity(message.tags)
            logger.info(f"🏷️  Проверка тегов для {message.product_id}: "
                       f"has_profanity={tags_check['has_profanity']}, "
                       f"found={tags_check['banned_words_found']}, "
                       f"tags={message.tags}")
            
            # Объединяем результаты проверок
            combined_check = combine_profanity_results(description_check, tags_check)
            
            # Логируем итоговый результат
            logger.info(f"🔎 Итоговая проверка для {message.product_id}: "
                       f"has_profanity={combined_check['has_profanity']}, "
                       f"found_words={combined_check['banned_words_found']}, "
                       f"count={combined_check['profanity_count']}")
            
            # Если есть запрещенные слова в описании или тегах - НЕ отправляем сообщение в shop-clear
            if combined_check['has_profanity']:
                stats['filtered'] += 1
                processing_stats['global'] = stats
                
                logger.warning(f"🚫 Продукт {message.product_id} ОТФИЛЬТРОВАН! Содержит запрещенных слов: {combined_check['profanity_count']}")
                
                if description_check['has_profanity']:
                    logger.warning(f"   Найдено в описании: {', '.join(description_check['banned_words_found'])}")
                    logger.warning(f"   Фрагмент описания: {message.description[:200]}...")
                
                if tags_check['has_profanity']:
                    logger.warning(f"   Найдено в тегах: {', '.join(tags_check['banned_words_found'])}")
                    logger.warning(f"   Теги: {message.tags}")
                
                continue  # Пропускаем это сообщение, НЕ отправляем в shop-clear
            
            # Если нет запрещенных слов - создаем отфильтрованное сообщение
            filtered_message = FilteredShopMessage(
                product_id=message.product_id,
                name=message.name,
                original_description=message.description,
                filtered_description=message.description,  # Тот же текст, так как нет запрещенных слов
                price=message.price,
                category=message.category,
                brand=message.brand,
                stock=message.stock,
                sku=message.sku,
                tags=message.tags,  # Оригинальные теги, так как нет запрещенных слов
                images=message.images,
                specifications=message.specifications,
                created_at=message.created_at,
                updated_at=message.updated_at,
                index=message.index,
                store_id=message.store_id,
                has_profanity=False,
                profanity_count=0,
                banned_words_found=[],
                filter_timestamp=datetime.now().isoformat()
            )
            
            # Отправляем в shop_clear только чистые сообщения
            await shop_clear_topic.send(value=filtered_message)
            
            # Обновляем статистику
            stats['clean'] += 1
            processing_stats['global'] = stats
            
            logger.info(f"✅ Продукт {message.product_id} прошел фильтрацию и отправлен в shop-clear")
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки сообщения о продукте {getattr(message, 'product_id', 'unknown')}: {e}", exc_info=True)


# ==================== WEB API ====================

@app.page('/api/v1/stats')
class StatsAPIView(View):
    async def get(self, request: Request) -> Response:
        """Получить статистику обработки"""
        try:
            stats = processing_stats.get('global', {'total': 0, 'filtered': 0, 'clean': 0})
            banned_words_count = len(banned_words_cache)
            
            # Получаем детальную информацию о запрещенных словах
            banned_words_list = []
            for word, info in banned_words_cache.items():
                banned_words_list.append({
                    'word': word,
                    'added_at': info.get('added_at'),
                    'source': info.get('source'),
                    'request_id': info.get('request_id')
                })
            
            return self.json({
                'status': 'success',
                'data': {
                    'processing_stats': stats,
                    'banned_words': {
                        'count': banned_words_count,
                        'words': banned_words_list
                    },
                    'service': 'shop-messages-filter',
                    'timestamp': datetime.now().isoformat()
                }
            })
        except Exception as e:
            logger.error(f"API GET /stats error: {e}", exc_info=True)
            return self.json({
                'status': 'error',
                'message': f'Ошибка при получении статистики: {str(e)}'
            }, status=500)


@app.page('/api/v1/banned-words')
class BannedWordsAPIView(View):
    async def get(self, request: Request) -> Response:
        """Получить текущий список запрещенных слов"""
        try:
            # Получаем все слова из кэша
            banned_words = []
            for word, info in banned_words_cache.items():
                banned_words.append({
                    'word': word,
                    'added_at': info.get('added_at'),
                    'source': info.get('source'),
                    'request_id': info.get('request_id')
                })
            
            return self.json({
                'status': 'success',
                'data': {
                    'words': banned_words,
                    'count': len(banned_words)
                }
            })
        except Exception as e:
            logger.error(f"API GET /banned-words error: {e}", exc_info=True)
            return self.json({
                'status': 'error',
                'message': f'Ошибка при получении списка слов: {str(e)}'
            }, status=500)


@app.page('/api/v1/banned-words/add')
class AddBannedWordAPIView(View):
    async def post(self, request: Request) -> Response:
        """Добавить запрещенное слово"""
        try:
            data = await request.json()
            word = data.get('word', '').strip()
            
            if not word:
                return self.json({
                    'status': 'error',
                    'message': 'Параметр "word" обязателен'
                }, status=400)
            
            # Отправляем событие в Kafka
            request_id = data.get('request_id', str(uuid.uuid4()))
            update = await send_ban_word_update(
                action='ADD',
                word=word,
                source='api',
                request_id=request_id
            )
            
            return self.json({
                'status': 'success',
                'message': f'Слово "{word}" добавлено в список запрещенных',
                'data': {
                    'word': word,
                    'added_at': update.timestamp,
                    'source': update.source,
                    'request_id': update.request_id
                }
            })
            
        except Exception as e:
            logger.error(f"API POST /banned-words/add error: {e}", exc_info=True)
            return self.json({
                'status': 'error',
                'message': f'Ошибка при добавлении слова: {str(e)}'
            }, status=500)


@app.page('/api/v1/banned-words/remove')
class RemoveBannedWordAPIView(View):
    async def post(self, request: Request) -> Response:
        """Удалить запрещенное слово"""
        try:
            data = await request.json()
            word = data.get('word', '').strip()
            
            if not word:
                return self.json({
                    'status': 'error',
                    'message': 'Параметр "word" обязателен'
                }, status=400)
            
            # Отправляем событие в Kafka
            request_id = data.get('request_id', str(uuid.uuid4()))
            update = await send_ban_word_update(
                action='REMOVE',
                word=word,
                source='api',
                request_id=request_id
            )
            
            return self.json({
                'status': 'success',
                'message': f'Слово "{word}" удалено из списка запрещенных',
                'data': {
                    'word': word,
                    'removed_at': update.timestamp,
                    'source': update.source,
                    'request_id': update.request_id
                }
            })
            
        except Exception as e:
            logger.error(f"API POST /banned-words/remove error: {e}", exc_info=True)
            return self.json({
                'status': 'error',
                'message': f'Ошибка при удалении слова: {str(e)}'
            }, status=500)


@app.page('/api/v1/banned-words/bulk-add')
class BulkAddBannedWordsAPIView(View):
    async def post(self, request: Request) -> Response:
        """Добавить несколько запрещенных слов"""
        try:
            data = await request.json()
            words = data.get('words', [])
            
            if not words or not isinstance(words, list):
                return self.json({
                    'status': 'error',
                    'message': 'Параметр "words" должен быть непустым списком'
                }, status=400)
            
            added_words = []
            skipped_words = []
            request_id = data.get('request_id', str(uuid.uuid4()))
            
            for word in words:
                try:
                    word_str = str(word).strip()
                    if not word_str:
                        continue
                    
                    # Отправляем событие в Kafka
                    update = await send_ban_word_update(
                        action='ADD',
                        word=word_str,
                        source='api-bulk',
                        request_id=request_id
                    )
                    
                    added_words.append({
                        'word': word_str,
                        'added_at': update.timestamp,
                        'request_id': update.request_id
                    })
                    
                except Exception as e:
                    logger.error(f"Ошибка при добавлении слова '{word}': {e}")
                    skipped_words.append({
                        'word': str(word),
                        'reason': f'Ошибка: {str(e)}'
                    })
            
            return self.json({
                'status': 'success',
                'message': f'Добавлено {len(added_words)} слов, пропущено {len(skipped_words)}',
                'data': {
                    'added': added_words,
                    'skipped': skipped_words,
                    'total_added': len(added_words),
                    'total_skipped': len(skipped_words)
                }
            })
            
        except Exception as e:
            logger.error(f"API POST /banned-words/bulk-add error: {e}", exc_info=True)
            return self.json({
                'status': 'error',
                'message': f'Ошибка при массовом добавлении слов: {str(e)}'
            }, status=500)


@app.page('/api/v1/banned-words/clear')
class ClearBannedWordsAPIView(View):
    async def post(self, request: Request) -> Response:
        """Очистить весь список запрещенных слов"""
        try:
            data = await request.json()
            confirm = data.get('confirm', False)
            
            if not confirm:
                return self.json({
                    'status': 'error',
                    'message': 'Подтверждение требуется. Отправьте {"confirm": true}'
                }, status=400)
            
            # Получаем все слова для удаления из кэша
            words_to_remove = list(banned_words_cache.keys())
            removed_count = 0
            request_id = str(uuid.uuid4())
            
            for word in words_to_remove:
                try:
                    # Отправляем событие в Kafka для каждого слова
                    await send_ban_word_update(
                        action='REMOVE',
                        word=word,
                        source='api-clear',
                        request_id=request_id
                    )
                    removed_count += 1
                    
                except Exception as e:
                    logger.error(f"Ошибка при удалении слова '{word}': {e}")
            
            return self.json({
                'status': 'success',
                'message': f'Удалено {removed_count} слов',
                'data': {
                    'removed_count': removed_count
                }
            })
            
        except Exception as e:
            logger.error(f"API POST /banned-words/clear error: {e}", exc_info=True)
            return self.json({
                'status': 'error',
                'message': f'Ошибка при очистке списка: {str(e)}'
            }, status=500)


@app.page('/api/v1/banned-words/check')
class CheckWordAPIView(View):
    async def get(self, request: Request) -> Response:
        """Проверить, является ли слово запрещенным (GET версия)"""
        try:
            word = request.query.get('word', '').strip()
            if not word:
                return self.json({
                    'status': 'error',
                    'message': 'Параметр "word" обязателен в query string'
                }, status=400)
            
            return await self._check_word(word)
            
        except Exception as e:
            logger.error(f"API GET /banned-words/check error: {e}", exc_info=True)
            return self.json({
                'status': 'error',
                'message': f'Ошибка при проверке слова: {str(e)}'
            }, status=500)
    
    async def post(self, request: Request) -> Response:
        """Проверить, является ли слово запрещенным (POST версия)"""
        try:
            data = await request.json()
            word = data.get('word', '').strip()
            
            if not word:
                return self.json({
                    'status': 'error',
                    'message': 'Параметр "word" обязателен'
                }, status=400)
            
            return await self._check_word(word)
            
        except Exception as e:
            logger.error(f"API POST /banned-words/check error: {e}", exc_info=True)
            return self.json({
                'status': 'error',
                'message': f'Ошибка при проверке слова: {str(e)}'
            }, status=500)
    
    async def _check_word(self, word: str) -> Response:
        """Внутренний метод проверки слова"""
        word_lower = word.lower()
        is_banned = word_lower in banned_words_cache
        
        word_info = None
        if is_banned:
            info = banned_words_cache[word_lower]
            word_info = info
        
        return self.json({
            'status': 'success',
            'data': {
                'word': word,
                'is_banned': is_banned,
                'info': word_info
            }
        })


@app.page('/api/v1/test-filter')
class TestFilterAPIView(View):
    async def post(self, request: Request) -> Response:
        """Протестировать фильтрацию текста"""
        try:
            data = await request.json()
            text = data.get('text', '')
            tags = data.get('tags', [])
            
            if not text and not tags:
                return self.json({
                    'status': 'error',
                    'message': 'Хотя бы один из параметров "text" или "tags" должен быть указан'
                }, status=400)
            
            # Тестируем фильтрацию
            text_result = check_text_for_profanity(text) if text else {'has_profanity': False, 'profanity_count': 0, 'banned_words_found': []}
            tags_result = check_tags_for_profanity(tags) if tags else {'has_profanity': False, 'profanity_count': 0, 'banned_words_found': []}
            
            combined_result = combine_profanity_results(text_result, tags_result)
            
            # Генерируем отфильтрованный текст для демонстрации
            filtered_text = text
            if text_result['has_profanity']:
                for word in text_result['banned_words_found']:
                    pattern = rf'\b{re.escape(word)}\b'
                    filtered_text = re.sub(pattern, '*' * len(word), filtered_text, flags=re.IGNORECASE)
            
            # Генерируем отфильтрованные теги для демонстрации
            filtered_tags = []
            if tags_result['has_profanity'] and tags:
                for tag in tags:
                    tag_str = str(tag)
                    temp_tag = tag_str
                    for word in tags_result['banned_words_found']:
                        pattern = rf'\b{re.escape(word)}\b'
                        temp_tag = re.sub(pattern, '*' * len(word), temp_tag, flags=re.IGNORECASE)
                    filtered_tags.append(temp_tag)
            else:
                filtered_tags = tags if tags else []
            
            return self.json({
                'status': 'success',
                'data': {
                    'original_text': text,
                    'filtered_text': filtered_text,
                    'original_tags': tags,
                    'filtered_tags': filtered_tags,
                    'text_check': text_result,
                    'tags_check': tags_result,
                    'combined_result': combined_result,
                    'total_banned_words': len(banned_words_cache),
                    'would_be_accepted': not combined_result['has_profanity']
                }
            })
        except Exception as e:
            logger.error(f"API POST /test-filter error: {e}", exc_info=True)
            return self.json({
                'status': 'error',
                'message': f'Ошибка при тестировании фильтрации: {str(e)}'
            }, status=500)


@app.page('/api/v1/check-product')
class CheckProductAPIView(View):
    async def post(self, request: Request) -> Response:
        """Проверить продукт на наличие запрещенных слов"""
        try:
            data = await request.json()
            
            # Проверяем описание и теги
            description = data.get('description', '')
            tags = data.get('tags', [])
            
            text_result = check_text_for_profanity(description)
            tags_result = check_tags_for_profanity(tags)
            combined_result = combine_profanity_results(text_result, tags_result)
            
            # Генерируем отфильтрованные данные для демонстрации
            filtered_text = description
            if text_result['has_profanity']:
                for word in text_result['banned_words_found']:
                    pattern = rf'\b{re.escape(word)}\b'
                    filtered_text = re.sub(pattern, '*' * len(word), filtered_text, flags=re.IGNORECASE)
            
            filtered_tags = []
            if tags_result['has_profanity'] and tags:
                for tag in tags:
                    tag_str = str(tag)
                    temp_tag = tag_str
                    for word in tags_result['banned_words_found']:
                        pattern = rf'\b{re.escape(word)}\b'
                        temp_tag = re.sub(pattern, '*' * len(word), temp_tag, flags=re.IGNORECASE)
                    filtered_tags.append(temp_tag)
            else:
                filtered_tags = tags if tags else []
            
            return self.json({
                'status': 'success',
                'data': {
                    'product_id': data.get('product_id', 'unknown'),
                    'name': data.get('name', ''),
                    'has_profanity': combined_result['has_profanity'],
                    'profanity_count': combined_result['profanity_count'],
                    'banned_words_found': combined_result['banned_words_found'],
                    'text_profanity': text_result,
                    'tags_profanity': tags_result,
                    'would_be_accepted': not combined_result['has_profanity'],
                    'description_preview': description[:100] + ('...' if len(description) > 100 else ''),
                    'filtered_description_preview': filtered_text[:100] + ('...' if len(filtered_text) > 100 else ''),
                    'tags': tags,
                    'filtered_tags': filtered_tags
                }
            })
        except Exception as e:
            logger.error(f"API POST /check-product error: {e}", exc_info=True)
            return self.json({
                'status': 'error',
                'message': f'Ошибка при проверке продукта: {str(e)}'
            }, status=500)


@app.page('/health')
class HealthCheckView(View):
    async def get(self, request: Request) -> Response:
        """Проверка здоровья сервиса"""
        try:
            health_status = {
                'status': 'healthy',
                'service': 'shop-messages-filter',
                'timestamp': datetime.now().isoformat(),
                'version': '1.0.0',
                'topics': {
                    'input': shop_raw_topic_name,
                    'output': shop_clear_topic_name,
                    'ban_words': ban_words_topic_name
                },
                'web_port': 6068,
                'banned_words_count': len(banned_words_cache),
                'processing_stats': processing_stats.get('global', {'total': 0, 'filtered': 0, 'clean': 0})
            }
            return self.json(health_status)
        except Exception as e:
            logger.error(f"Health check error: {e}", exc_info=True)
            return self.json({
                'status': 'unhealthy',
                'error': str(e)
            }, status=503)


@app.page('/')
class IndexView(View):
    async def get(self, request: Request) -> Response:
        """Главная страница с документацией API"""
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Shop Messages Filter - API Documentation</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
                h1 { color: #333; border-bottom: 2px solid #eee; padding-bottom: 10px; }
                h2 { color: #555; margin-top: 30px; }
                .endpoint { background: #f9f9f9; padding: 20px; margin: 15px 0; border-left: 4px solid #4CAF50; border-radius: 5px; }
                .method { display: inline-block; padding: 5px 12px; border-radius: 4px; color: white; font-weight: bold; margin-right: 10px; }
                .get { background: #61affe; }
                .post { background: #49cc90; }
                code { background: #e8e8e8; padding: 3px 6px; border-radius: 3px; font-family: monospace; }
                .example { background: #f5f5f5; padding: 15px; margin: 10px 0; border-radius: 5px; }
                pre { background: #f5f5f5; padding: 15px; border-radius: 5px; overflow-x: auto; }
                .current-stats { background: #e8f5e9; padding: 15px; border-radius: 5px; margin: 20px 0; }
            </style>
        </head>
        <body>
            <h1>Shop Messages Filter API</h1>
            <p>Фильтрация описаний продуктов из <code>shop_raw</code> в <code>shop_clear</code></p>
            <p><strong>ВАЖНО:</strong> Сообщения с запрещенными словами НЕ попадают в shop-clear!</p>
            <p><strong>Проверяемые поля:</strong> описание (description) и теги (tags)</p>
            
            <div id="currentStats" class="current-stats">
                <h3>📊 Текущая статистика</h3>
                <p>Загрузка...</p>
            </div>
            
            <h2>🚫 Управление запрещенными словами</h2>
            
            <div class="endpoint">
                <span class="method get">GET</span>
                <strong><code>/api/v1/banned-words</code></strong>
                <p>Получить текущий список запрещенных слов</p>
            </div>
            
            <div class="endpoint">
                <span class="method post">POST</span>
                <strong><code>/api/v1/banned-words/add</code></strong>
                <p>Добавить запрещенное слово</p>
                <div class="example">
                    <strong>Пример:</strong><br>
                    <pre>curl -X POST http://localhost:6068/api/v1/banned-words/add \\
  -H "Content-Type: application/json" \\
  -d '{"word": "плохой"}'</pre>
                </div>
            </div>
            
            <div class="endpoint">
                <span class="method post">POST</span>
                <strong><code>/api/v1/banned-words/remove</code></strong>
                <p>Удалить запрещенное слово</p>
                <div class="example">
                    <strong>Пример:</strong><br>
                    <pre>curl -X POST http://localhost:6068/api/v1/banned-words/remove \\
  -H "Content-Type: application/json" \\
  -d '{"word": "плохой"}'</pre>
                </div>
            </div>
            
            <div class="endpoint">
                <span class="method post">POST</span>
                <strong><code>/api/v1/banned-words/bulk-add</code></strong>
                <p>Добавить несколько запрещенных слов</p>
                <div class="example">
                    <strong>Пример:</strong><br>
                    <pre>curl -X POST http://localhost:6068/api/v1/banned-words/bulk-add \\
  -H "Content-Type: application/json" \\
  -d '{"words": ["плохой", "ужасный", "кошмар"]}'</pre>
                </div>
            </div>
            
            <div class="endpoint">
                <span class="method post">POST</span>
                <strong><code>/api/v1/banned-words/clear</code></strong>
                <p>Очистить весь список запрещенных слов</p>
                <div class="example">
                    <strong>Пример:</strong><br>
                    <pre>curl -X POST http://localhost:6068/api/v1/banned-words/clear \\
  -H "Content-Type: application/json" \\
  -d '{"confirm": true}'</pre>
                </div>
            </div>
            
            <div class="endpoint">
                <span class="method get">GET</span>
                <strong><code>/api/v1/banned-words/check?word=плохой</code></strong>
                <p>Проверить, является ли слово запрещенным (GET)</p>
            </div>
            
            <div class="endpoint">
                <span class="method post">POST</span>
                <strong><code>/api/v1/banned-words/check</code></strong>
                <p>Проверить, является ли слово запрещенным (POST)</p>
            </div>
            
            <h2>📊 Статистика</h2>
            
            <div class="endpoint">
                <span class="method get">GET</span>
                <strong><code>/api/v1/stats</code></strong>
                <p>Получить статистику обработки</p>
            </div>
            
            <h2>🔍 Тестирование</h2>
            
            <div class="endpoint">
                <span class="method post">POST</span>
                <strong><code>/api/v1/test-filter</code></strong>
                <p>Протестировать фильтрацию текста и теги</p>
                <div class="example">
                    <strong>Пример:</strong><br>
                    <pre>curl -X POST http://localhost:6068/api/v1/test-filter \\
  -H "Content-Type: application/json" \\
  -d '{"text": "Отличный продукт", "tags": ["качество", "премиум"]}'</pre>
                </div>
            </div>
            
            <div class="endpoint">
                <span class="method post">POST</span>
                <strong><code>/api/v1/check-product</code></strong>
                <p>Проверить продукт на запрещенные слова</p>
            </div>
            
            <h2>❤️ Health Check</h2>
            
            <div class="endpoint">
                <span class="method get">GET</span>
                <strong><code>/health</code></strong>
                <p>Проверка здоровья сервиса</p>
            </div>
            
            <script>
                // Загружаем текущую статистику
                fetch('/api/v1/stats')
                    .then(response => response.json())
                    .then(data => {
                        if (data.status === 'success') {
                            const stats = data.data;
                            const statsDiv = document.getElementById('currentStats');
                            statsDiv.innerHTML = `
                                <h3>📊 Текущая статистика</h3>
                                <p><strong>Всего обработано сообщений:</strong> ${stats.processing_stats.total}</p>
                                <p><strong>Очищено сообщений:</strong> ${stats.processing_stats.clean}</p>
                                <p><strong>Отфильтровано сообщений:</strong> ${stats.processing_stats.filtered}</p>
                                <p><strong>Запрещенных слов:</strong> ${stats.banned_words.count}</p>
                                <p><small>Обновлено: ${new Date().toLocaleTimeString()}</small></p>
                            `;
                        }
                    })
                    .catch(error => {
                        console.error('Error loading stats:', error);
                    });
            </script>
        </body>
        </html>
        """
        return self.html(html_content)


if __name__ == '__main__':
    app.main()