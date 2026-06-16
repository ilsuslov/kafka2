import os
import re
import logging
import faust

# Конфиг через ENV, чтобы не хардкодить адрес брокера
BROKER = os.getenv("KAFKA_BROKER", "kafka://localhost:9092")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

app = faust.App(
    "stream_processor",
    broker=BROKER,
    # RocksDB держит таблицы на диске. Данные не потеряются при рестарте воркера
    store="rocksdb://",
    value_serializer="json"
)

# Топики строго по ТЗ
messages_in = app.topic("messages", value_type=dict)
messages_out = app.topic("filtered_messages", value_type=dict)
control_topic = app.topic("blocked_users", value_type=dict)

# Персистентные таблицы состояния
# Ключ: recipient_id -> Значение: список заблокированных sender_id
blocked_users = app.Table("blocked_users", default=list)
# Ключ: "global" -> Значение: список запрещённых слов
banned_words = app.Table("banned_words", default=list)


@app.agent(control_topic)
async def handle_control_events(stream):
    """
    Один агент управляет и блокировками, и цензурой.
    Faust автоматически пишет изменения таблиц в Kafka-changelog,
    поэтому состояние реплицируется и сохраняется.
    """
    async for event in stream:
        action = event.get("action")
        if not action:
            continue

        # --- Управление блокировками ---
        if action in ("block", "unblock"):
            uid, target = event.get("user_id"), event.get("target_id")
            if not (uid and target):
                logger.warning("Пропущено событие блокировки: нет user_id или target_id")
                continue

            current = blocked_users[uid]
            if action == "block" and target not in current:
                # Важно: в Faust нужно присваивать новый список, чтобы сработал changelog
                blocked_users[uid] = list(set(current + [target]))
                logger.info(f"🔒 {uid} заблокировал {target}")
            elif action == "unblock" and target in current:
                blocked_users[uid] = [x for x in current if x != target]
                logger.info(f"🔓 {uid} разблокировал {target}")

        # --- Управление цензурой ---
        elif action in ("add_word", "remove_word"):
            word = event.get("word")
            if not word:
                continue

            current = banned_words["global"]
            if action == "add_word" and word not in current:
                banned_words["global"] = list(set(current + [word]))
                logger.info(f"🚫 Добавлено слово в банлист: '{word}'")
            elif action == "remove_word" and word in current:
                banned_words["global"] = [x for x in current if x != word]
                logger.info(f"✅ Удалено слово из банлиста: '{word}'")


@app.agent(messages_in)
async def process_and_filter(stream):
    """
    Основной пайплайн:
    1. Проверка, не в блоке ли отправитель у получателя
    2. Замена запрещённых слов на ***
    3. Отправка в filtered_messages
    """
    async for msg in stream:
        sender, recipient = msg.get("sender"), msg.get("recipient")
        content = msg.get("content", "")

        # 1. Блокировка
        if sender in blocked_users[recipient]:
            logger.info(f"⛔ Сообщение от {sender} для {recipient} дропнуто (в блоке)")
            continue

        # 2. Цензура
        words = banned_words["global"]
        if words:
            # \b гарантирует замену целых слов, re.IGNORECASE — регистронезависимо
            pattern = re.compile(r'\b(' + '|'.join(map(re.escape, words)) + r')\b', re.IGNORECASE)
            content = pattern.sub("***", content)
            msg["censored"] = True
        else:
            msg["censored"] = False

        msg["content"] = content
        await messages_out.send(value=msg)
        logger.info(f"📤 Сообщение {sender}->{recipient} прошло фильтрацию")
