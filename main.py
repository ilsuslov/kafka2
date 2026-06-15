import faust
import json
import re
import time

# коннект к кафке
app = faust.App(
    'stream_filter_v2',
    broker='kafka://localhost:9092',
    value_serializer='raw'
)

# топики
t_msgs = app.topic('messages', value_serializer='raw')
t_out = app.topic('filtered_messages', value_serializer='raw')
t_blk = app.topic('blocked_users', value_serializer='raw')
t_cens = app.topic('censored_words', value_serializer='raw')

# таблицы (rocksdb под капотом, данные не слетают при рестарте)
# default=lambda: [] чтобы не было KeyError если юзер первый раз
blocked_db = app.Table('blocked_users', default=lambda: [])
words_db = app.Table('censored_dict')

# кэш для регекса, чтобы не собирать каждый раз (ну, почти)
_cached_regex = None

def get_regex():
    global _cached_regex
    words = list(words_db.values())
    if not words:
        return None
    
    # сортируем чтобы длинные слова шли первыми, иначе 'мат' съест 'матрос'
    words = sorted(words, key=len, reverse=True)
    pattern = r'\b(' + '|'.join(re.escape(w) for w in words) + r')\b'
    _cached_regex = re.compile(pattern, re.IGNORECASE)
    return _cached_regex

@app.agent(t_blk)
async def handle_blocks(stream):
    async for msg in stream:
        try:
            d = json.loads(msg)
            rid = d.get('recipient')
            blocked = d.get('blocked', [])
            if rid:
                blocked_db[rid] = blocked
                print(f"[BLOCK] {rid} теперь блочит: {blocked}")
        except Exception as e:
            print(f"[ERR] блок: {e}")

@app.agent(t_cens)
async def handle_words(stream):
    async for msg in stream:
        try:
            d = json.loads(msg)
            w = d.get('word', '').strip().lower()
            if w:
                words_db[w] = w
                print(f"[CENS] добавлено слово: {w}")
                # сбрасываем кэш, чтобы новый regex собрался
                global _cached_regex
                _cached_regex = None
        except Exception as e:
            print(f"[ERR] слово: {e}")

@app.agent(t_msgs)
async def process(stream):
    async for raw in stream:
        try:
            data = json.loads(raw)
            sender = data['sender']
            rec = data['recipient']
            text = data['text']

            # 1. проверка блокировки
            if sender in blocked_db[rec]:
                print(f"🚫 dropped: {sender} -> {rec}")
                continue

            # 2. цензура
            regex = get_regex()
            if regex:
                text = regex.sub('***', text)

            data['text'] = text
            await t_out.send(
                key=rec.encode(),
                value=json.dumps(data, ensure_ascii=False).encode()
            )
            print(f"✅ passed: {sender} -> {rec}")
        except KeyError as e:
            print(f"❌ missing key {e} in message, skip")
        except Exception as e:
            print(f"💥 crash processing: {e}")

if __name__ == '__main__':
    # кафка иногда тупит на старте, даем ей 2 сек
    time.sleep(2)
    print("🚀 starting worker...")
    app.main()
