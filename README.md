 Потоковая обработка сообщений (Faust + Kafka)


 Стек
- Kafka 3.7** (KRaft mode, без ZooKeeper)
- Faust 1.10** (стриминг на Python)
- RocksDB** (персистентное хранение таблиц состояния)
- Docker Compose** (развёртывание)

 Структура топиков 
Топик  Назначение 

`messages`  Входящие сообщения `{"sender":"...", "recipient":"...", "content":"..."}` 
 `filtered_messages`  Выход после обработки 
`blocked_users`  Управляющие события (блокировки + обновление стоп-слов) 

 Как запустить
```bash
1. Поднять всё
docker-compose up -d --build

2. Проверить, что воркер запустился (должно быть "faust: worker started")
docker logs -f worker


3. Настроить правила
docker exec kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic blocked_users

# Вводим построчно (Enter после каждой строки):
{"action": "add_word", "word": "спам"}
{"action": "add_word", "word": "плохое"}
{"action": "block", "user_id": "user2", "target_id": "user1"}
# Ctrl+C для выхода


4. Отправка сообщения

docker exec kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic messages

{"sender": "user1", "recipient": "user2", "content": "Привет! Это тестовое сообщение."}
{"sender": "user3", "recipient": "user4", "content": "Не отправляй спам, это плохое поведение."}
{"sender": "user3", "recipient": "user2", "content": "Добрый день, как дела?"}
# Ctrl+C

5. Проверка результата

docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic filtered_messages \
  --from-beginning

6. Вывод
{"sender":"user3","recipient":"user4","content":"Не отправляй ***, это *** поведение.","censored":true}
{"sender":"user3","recipient":"user2","content":"Добрый день, как дела?","censored":false}

Сообщение user1 -> user2 отсутствует (сработала блокировка).
Слова спам и плохое заменены на *** (сработала цензура).

Заметки по реализации
Персистентность: Использую store="rocksdb://". Faust сам пишет changelog-топики, поэтому при рестарте worker таблицы восстанавливаются автоматически.
Обновление таблиц: В Faust нельзя делать table[key].append(). Нужно присваивать новый список целиком, иначе изменения не попадут в changelog. Это учтено в handle_control_events.
KRaft vs ZooKeeper: Убрал ZK, т.к. в Kafka 3.5+ он deprecated. Экономит ресурсы и упрощает docker-compose.
В проде: Для продакшена стоит вынести blocked_users и banned_words в отдельные топики, добавить dead-letter queue и мониторинг lag'а.
