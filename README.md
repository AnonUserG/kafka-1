# Сервис обработки потоков сообщений

Практическая работа: система чата с блокировкой пользователей и цензурой сообщений на базе **Kafka Streams**.

---

## Архитектура

```
[messages] ──► leftJoin(blocked_users) ──► filter ──► processValues(цензура) ──► [filtered_messages]
                       ▲                                        ▲
               [blocked_users]                           [banned_words]
               (GlobalKTable)                            (GlobalKTable)
```

| Топик             | Назначение                              | Ключ                      | Значение          |
|-------------------|-----------------------------------------|---------------------------|-------------------|
| `messages`        | Входящие сообщения                      | `senderId`                | `ChatMessage` JSON |
| `filtered_messages` | Обработанные сообщения               | `senderId`                | `ChatMessage` JSON |
| `blocked_users`   | Список блокировок                       | `"recipientId:senderId"`  | `true` / `false`  |
| `banned_words`    | Список запрещённых слов                 | слово (lowercase)         | `true` / `false`  |

---

## Описание классов

### `model/ChatMessage`
Record-модель сообщения: `id`, `senderId`, `recipientId`, `text`, `timestamp`.

### `config/KafkaStreamsConfig`
- Аннотация `@EnableKafkaStreams` — включает поддержку Kafka Streams в Spring.
- Четыре бина `NewTopic` — создают топики через AdminClient при старте приложения.

### `streams/MessagingTopology`
Строит Kafka Streams топологию:

1. **`GlobalKTable` `blocked_users`** — персистентный RocksDB-стор `blocked-users-store`.  
   Ключ `"recipientId:senderId"` → `true` означает, что получатель заблокировал отправителя.  
   Данные хранятся на диске и восстанавливаются после перезапуска.

2. **`GlobalKTable` `banned_words`** — персистентный RocksDB-стор `banned-words-store`.  
   Ключ — слово в нижнем регистре, значение `true` — слово запрещено.  
   Обновляется динамически: новые записи в топике немедленно отражаются в сторе.

3. **Поток `messages`**:
   - `leftJoin` с `blocked_users` по ключу `"recipientId:senderId"` → если `true`, сообщение заменяется `null`.
   - `filter` — удаляет `null` (заблокированные сообщения).
   - `processValues(CensorshipProcessor)` — применяет цензуру.
   - `to("filtered_messages")` — публикует результат.

### `streams/CensorshipProcessor`
Реализует `FixedKeyProcessor<String, ChatMessage, ChatMessage>`.  
В методе `process()` итерируется по всем записям `banned-words-store` и заменяет совпадения маской `***` (звёздочек столько, сколько букв в слове). Замена нечувствительна к регистру.

---

## Запуск проекта

### Предварительные требования
- Docker Desktop (или Docker Engine + Compose)
- Java 21, Maven 3.9+ (только если запускаете из IDE)

### Шаги

```bash
# 1. Собрать и запустить всё
docker compose up -d --build

# 2. Проверить, что все сервисы запущены
docker compose ps
```

После старта:
- **Kafka UI** — http://localhost:8080 (просмотр топиков и сообщений)
- Приложение автоматически создаёт все 4 топика и запускает Streams-обработку.

---

## Инструкция по тестированию

Все команды выполняются с **хоста** через порт `29092` (внешний listener).

### 1. Добавить запрещённые слова

```bash
# Добавить слово "спам" в список запрещённых
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:29092 \
  --topic banned_words \
  --property "parse.key=true" \
  --property "key.separator=:" \
  --property value.serializer=org.apache.kafka.common.serialization.BooleanSerializer \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer
```

> Так как `kafka-console-producer` не поддерживает Boolean-значения напрямую,  
> используйте **Kafka UI**: Topics → `banned_words` → Produce Message.  
> Ключ: `спам`, Значение: `true`.

Добавьте через Kafka UI следующие слова (ключ → значение):

| Ключ       | Значение |
|------------|----------|
| `спам`     | `true`   |
| `реклама`  | `true`   |
| `плохое`   | `true`   |

### 2. Заблокировать пользователя

Через Kafka UI → Topics → `blocked_users` → Produce Message:

| Ключ            | Значение | Смысл                          |
|-----------------|----------|--------------------------------|
| `alice:bob`     | `true`   | alice заблокировала bob        |
| `charlie:dave`  | `true`   | charlie заблокировал dave      |

### 3. Отправить тестовые сообщения

Через Kafka UI → Topics → `messages` → Produce Message.  
Ключ — любой (например, `senderId`), значение — JSON:

**Сообщение 1** — должно пройти фильтр и попасть в `filtered_messages`:
```json
{
  "id": "msg-001",
  "senderId": "charlie",
  "recipientId": "alice",
  "text": "Привет, Alice! Как дела?",
  "timestamp": "2026-05-24T10:00:00Z"
}
```

**Сообщение 2** — должно быть **заблокировано** (bob → alice):
```json
{
  "id": "msg-002",
  "senderId": "bob",
  "recipientId": "alice",
  "text": "Это сообщение от bob",
  "timestamp": "2026-05-24T10:01:00Z"
}
```

**Сообщение 3** — должно пройти с **цензурой** (слово "спам" замаскируется):
```json
{
  "id": "msg-003",
  "senderId": "charlie",
  "recipientId": "alice",
  "text": "Это не спам и не реклама!",
  "timestamp": "2026-05-24T10:02:00Z"
}
```

**Сообщение 4** — заблокировано (dave → charlie):
```json
{
  "id": "msg-004",
  "senderId": "dave",
  "recipientId": "charlie",
  "text": "Привет от dave",
  "timestamp": "2026-05-24T10:03:00Z"
}
```

### 4. Проверить результаты

Откройте Kafka UI → Topics → `filtered_messages`.

Ожидаемый результат:

| ID сообщения | Ожидание                                      |
|--------------|-----------------------------------------------|
| `msg-001`    | Присутствует, текст без изменений             |
| `msg-002`    | **Отсутствует** (bob заблокирован alice)      |
| `msg-003`    | Присутствует, текст: `"Это не **** и не ********!"` |
| `msg-004`    | **Отсутствует** (dave заблокирован charlie)   |

### 5. Динамическое обновление запрещённых слов

Добавьте через Kafka UI в `banned_words` новую запись: ключ `привет`, значение `true`.  
Отправьте в `messages`:
```json
{
  "id": "msg-005",
  "senderId": "charlie",
  "recipientId": "alice",
  "text": "Привет снова!",
  "timestamp": "2026-05-24T10:05:00Z"
}
```
В `filtered_messages` текст станет: `"****** снова!"` — без перезапуска приложения.

---

## Конфигурация

| Параметр                          | Значение по умолчанию | Переменная окружения        |
|-----------------------------------|-----------------------|-----------------------------|
| Kafka bootstrap servers           | `localhost:9092`      | `KAFKA_BOOTSTRAP_SERVERS`   |
| Kafka Streams application ID      | `messaging-streams-app` | —                          |
| Replication factor (Streams)      | `1`                   | —                           |

> Для подключения к продакшн-кластеру (3 брокера) установите:  
> `KAFKA_BOOTSTRAP_SERVERS=proh-vm-tsearch:9092,prmo-ocr:9092,uvt-vm-opg3:9092`  
> и измените `replication-factor: 3` в `application.yaml`.

---

## Остановка

```bash
docker compose down          # остановить контейнеры
docker compose down -v       # остановить и удалить данные Kafka (полный сброс)
```
