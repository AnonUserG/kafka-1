# Сервис обработки потоков сообщений

## Шаг 1. Запуск

```bash
docker compose up -d --build
```

После старта **Kafka UI** доступен по адресу http://localhost:8888.

---

## Шаг 2. Тестирование

### 2.1 Добавить запрещённые слова

Kafka UI → Topics → `banned_words` → Produce Message:

| Ключ       | Значение |
|------------|----------|
| `спам`     | `true`   |
| `реклама`  | `true`   |
| `плохое`   | `true`   |

### 2.2 Заблокировать пользователя

Kafka UI → Topics → `blocked_users` → Produce Message:

| Ключ            | Значение | Смысл                     |
|-----------------|----------|---------------------------|
| `alice:bob`     | `true`   | alice заблокировала bob   |
| `charlie:dave`  | `true`   | charlie заблокировал dave |

### 2.3 Отправить тестовые сообщения

Kafka UI → Topics → `messages` → Produce Message.
Вставьте содержимое напрямую:

> Ключ Kafka-сообщения не влияет на фильтрацию — логика блокировки и цензуры читает поля из тела. Используйте `senderId` как конвенцию.

**Сообщение 1** — Key: `charlie`, должно пройти без изменений:
```json
{"id": "msg-001", "senderId": "charlie", "recipientId": "alice", "text": "Привет, Alice! Как дела?", "timestamp": "2026-05-24T10:00:00Z"}
```

**Сообщение 2** — Key: `bob`, должно быть заблокировано (bob → alice):
```json
{"id": "msg-002", "senderId": "bob", "recipientId": "alice", "text": "Это сообщение от bob", "timestamp": "2026-05-24T10:01:00Z"}
```

**Сообщение 3** — Key: `charlie`, должно пройти с цензурой:
```json
{"id": "msg-003", "senderId": "charlie", "recipientId": "alice", "text": "Это не спам и не реклама!", "timestamp": "2026-05-24T10:02:00Z"}
```

**Сообщение 4** — Key: `dave`, должно быть заблокировано (dave → charlie):
```json
{"id": "msg-004", "senderId": "dave", "recipientId": "charlie", "text": "Привет от dave", "timestamp": "2026-05-24T10:03:00Z"}
```

### 2.4 Проверить результаты

Kafka UI → Topics → `filtered_messages`:

| ID        | Ожидание                                    |
|-----------|---------------------------------------------|
| `msg-001` | Присутствует без цензуры                    |
| `msg-002` | **Отсутствует** (bob заблокирован alice)    |
| `msg-003` | Присутствует  с цензурой                    |
| `msg-004` | **Отсутствует** (dave заблокирован charlie) |

### 2.5 Проверить динамическое обновление запрещённых слов

Добавьте в `banned_words`: ключ `привет`, значение `true`. Затем отправьте в `messages`:

Для теста с любым key
```json
{"id": "msg-005", "senderId": "charlie", "recipientId": "alice", "text": "Привет снова!", "timestamp": "2026-05-24T10:05:00Z"}
```
Сообщение попадет в `filtered_messages` — без перезапуска приложения и слово будет зацензурено на *******.

---

## Остановка
```bash
docker compose down -v   # остановить и удалить данные (полный сброс)
```
