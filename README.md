# Kafka practical work on Java 21

Проект содержит:

- `OrderScheduler` - producer на `@Scheduled`
- `SingleMessageConsumer` - consumer по одному сообщению
- `BatchMessageConsumer` - batch consumer с `commitSync()`
- `Order` - DTO сообщения в формате Java `record`

## Конфигурация

Основные настройки находятся в [application.yml](src/main/resources/application.yaml):

- `spring.kafka.bootstrap-servers`
- `spring.kafka.consumer.key-deserializer`
- `spring.kafka.consumer.value-deserializer`
- `spring.kafka.producer.key-serializer`
- `spring.kafka.producer.value-serializer`
- `spring.kafka.producer.acks`
- `spring.kafka.producer.retries`
- `spring.kafka.listener.ack-mode`
- `app.kafka.single-consumer.group-id`
- `app.kafka.batch-consumer.group-id`
- `app.kafka.batch-consumer.max-poll-records`
- `app.kafka.batch-consumer.fetch-min-bytes`
- `app.kafka.batch-consumer.fetch-max-wait-ms`

## Как работает

`OrderScheduler` раз в `fixed-delay-ms` создаёт новый `Order` и отправляет его в Kafka через `KafkaTemplate`.

`SingleMessageConsumer` читает сообщения по одному. Для него включён auto commit.

`BatchMessageConsumer` читает минимум по 10 сообщений за один poll, обрабатывает их в цикле и один раз вызывает `commitSync()` после обработки пачки.

Оба consumer имеют разные `group-id`, поэтому они могут параллельно читать одни и те же сообщения.

## Запуск

1. Поднимите Kafka-кластер и Kafka UI:

```bash
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-ui
```

2. Создайте топик:

```bash
docker exec -it kafka-1 kafka-topics --create --topic my-topic --partitions 3 --replication-factor 2 --bootstrap-server localhost:9092
```

3. Запустите приложение в двух экземплярах:

```bash
docker compose up -d --build kafka-app
```

Если запускаете из IDE, приложение подключается к:

```
localhost:19092,localhost:19093,localhost:19094
```



