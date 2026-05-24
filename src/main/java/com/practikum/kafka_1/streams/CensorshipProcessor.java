package com.practikum.kafka_1.streams;

import com.practikum.kafka_1.model.ChatMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.regex.Pattern;

// Процессор цензуры: заменяет запрещённые слова маской из звёздочек.
// Список слов читается из персистентного GlobalKTable-стора (RocksDB),
// поэтому изменения вступают в силу без перезапуска приложения.
@Slf4j
public class CensorshipProcessor implements FixedKeyProcessor<String, ChatMessage, ChatMessage> {

    private FixedKeyProcessorContext<String, ChatMessage> context;
    private ReadOnlyKeyValueStore<String, Boolean> bannedWordsStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(FixedKeyProcessorContext<String, ChatMessage> context) {
        this.context = context;
        // Глобальные сторы доступны из любого процессора без явного объявления зависимости
        this.bannedWordsStore = (ReadOnlyKeyValueStore<String, Boolean>)
                context.getStateStore(MessagingTopology.BANNED_WORDS_STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, ChatMessage> record) {
        ChatMessage message = record.value();
        String original = message.text();
        String censored = maskBannedWords(original);

        if (!censored.equals(original)) {
            log.info("Censored message id={} from={}: '{}' → '{}'",
                    message.id(), message.senderId(), original, censored);
        }

        context.forward(record.withValue(new ChatMessage(
                message.id(),
                message.senderId(),
                message.recipientId(),
                censored,
                message.timestamp()
        )));
    }

    // Перебирает все запрещённые слова из стора и заменяет совпадения маской.
    // Замена нечувствительна к регистру и учитывает границы слов.
    private String maskBannedWords(String text) {
        try (KeyValueIterator<String, Boolean> it = bannedWordsStore.all()) {
            while (it.hasNext()) {
                KeyValue<String, Boolean> entry = it.next();
                if (Boolean.TRUE.equals(entry.value)) {
                    String word = entry.key;
                    String mask = "*".repeat(word.length());
                    // (?i) — без учёта регистра; \b — граница слова
                    text = text.replaceAll("(?i)\\b" + Pattern.quote(word) + "\\b", mask);
                }
            }
        }
        return text;
    }

    @Override
    public void close() {}
}
