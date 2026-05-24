package com.practikum.kafka_1.streams;

import com.practikum.kafka_1.model.ChatMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;

// Kafka Streams топология обработки сообщений.
//
// Граф потока:
//   [messages] ──► leftJoin(blocked_users) ──► filter(null) ──► processValues(цензура) ──► [filtered_messages]
//                        ▲                                              ▲
//                [blocked_users]                                  [banned_words]
//                (GlobalKTable)                                   (GlobalKTable)
@Configuration
public class MessagingTopology {

    // Имена персистентных RocksDB-сторов, используемых GlobalKTable.
    // Оба стора восстанавливаются после перезапуска из Kafka-топиков.
    public static final String BLOCKED_USERS_STORE = "blocked-users-store";
    public static final String BANNED_WORDS_STORE  = "banned-words-store";

    @Bean
    public KStream<String, ChatMessage> messageStream(StreamsBuilder builder) {
        JsonSerde<ChatMessage> messageSerde = buildMessageSerde();

        // ── Персистентное хранилище блокировок ────────────────────────────────
        // GlobalKTable реплицируется на каждый экземпляр приложения.
        // Ключ: "recipientId:senderId" → значение: true (заблокирован) / false.
        // Чтобы заблокировать: отправьте ключ="alice:bob", value=true.
        // Чтобы разблокировать: отправьте ключ="alice:bob", value=false (или tombstone).
        GlobalKTable<String, Boolean> blockedUsersTable = builder.globalTable(
                "blocked_users",
                Consumed.with(Serdes.String(), Serdes.Boolean()),
                Materialized.<String, Boolean, KeyValueStore<Bytes, byte[]>>as(BLOCKED_USERS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Boolean())
        );

        // ── Персистентное хранилище запрещённых слов ──────────────────────────
        // Ключ: слово в нижнем регистре → значение: true (запрещено) / false.
        // Список обновляется динамически: новые записи вступают в силу сразу,
        // без перезапуска приложения.
        // Возвращаемое значение не используется для join — стор нужен
        // CensorshipProcessor, который получает его через context.getStateStore().
        builder.globalTable(
                "banned_words",
                Consumed.with(Serdes.String(), Serdes.Boolean()),
                Materialized.<String, Boolean, KeyValueStore<Bytes, byte[]>>as(BANNED_WORDS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Boolean())
        );

        // ── Поток входящих сообщений ──────────────────────────────────────────
        KStream<String, ChatMessage> messages = builder.stream(
                "messages",
                Consumed.with(Serdes.String(), messageSerde)
        );

        messages
                // Шаг 1 — Блокировка нежелательных отправителей.
                // leftJoin формирует ключ "recipientId:senderId" и ищет его в сторе.
                // Если запись найдена и равна true — сообщение заменяется на null (tombstone).
                // Если запись не найдена (пользователь не заблокирован) — globalValue = null,
                // Boolean.TRUE.equals(null) = false → сообщение пропускается.
                .leftJoin(
                        blockedUsersTable,
                        (msgKey, msg) -> msg.recipientId() + ":" + msg.senderId(),
                        (msg, isBlocked) -> Boolean.TRUE.equals(isBlocked) ? null : msg
                )
                // Убираем tombstone-записи (сообщения от заблокированных пользователей)
                .filter((key, msg) -> msg != null)
                // Шаг 2 — Цензура запрещённых слов через CensorshipProcessor.
                // Процессор читает banned-words-store и маскирует совпадения звёздочками.
                .processValues(CensorshipProcessor::new)
                // Результат — в топик filtered_messages
                .to("filtered_messages", Produced.with(Serdes.String(), messageSerde));

        return messages;
    }

    // Serde для ChatMessage: не добавляет и не проверяет type-заголовки,
    // поэтому внешние producer'ы (kafka-console-producer, тесты) могут
    // отправлять чистый JSON без специальных заголовков.
    private JsonSerde<ChatMessage> buildMessageSerde() {
        JsonSerde<ChatMessage> serde = new JsonSerde<>(ChatMessage.class);
        serde.configure(
                Map.of(
                        JsonDeserializer.TRUSTED_PACKAGES, "com.practikum.kafka_1.model",
                        JsonDeserializer.USE_TYPE_INFO_HEADERS, false
                ),
                false
        );
        return serde;
    }
}
