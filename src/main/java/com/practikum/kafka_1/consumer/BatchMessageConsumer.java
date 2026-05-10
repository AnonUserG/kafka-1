package com.practikum.kafka_1.consumer;

import com.practikum.kafka_1.model.Order;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class BatchMessageConsumer {

	@KafkaListener(
			topics = "${app.kafka.topic:my-topic}",
			groupId = "${app.kafka.batch-consumer.group-id:batch-message-consumer-group}",
			containerFactory = "batchKafkaListenerContainerFactory",
			properties = {
					"enable.auto.commit=${app.kafka.batch-consumer.enable-auto-commit:false}",
					"max.poll.records=${app.kafka.batch-consumer.max-poll-records:10}",
					"fetch.min.bytes=${app.kafka.batch-consumer.fetch-min-bytes:1024}",
					"fetch.max.wait.ms=${app.kafka.batch-consumer.fetch-max-wait-ms:5000}"
			}
	)
	public void listen(List<Order> orders, Consumer<?, ?> consumer) {
		try {
			log.info("BatchMessageConsumer received batch with {} records", orders.size());

			for (Order order : orders) {
				try {
					log.info("BatchMessageConsumer processed order: {}", order);
				}
				catch (Exception exception) {
					log.error("BatchMessageConsumer error while processing order {}: {}", order, exception.getMessage(), exception);
				}
			}

			try {
				consumer.commitSync();
				log.info("BatchMessageConsumer committed offsets after processing {} records", orders.size());
			}
			catch (Exception exception) {
				log.error("BatchMessageConsumer error while committing offsets: {}", exception.getMessage(), exception);
			}
		}
		catch (Exception exception) {
			log.error("BatchMessageConsumer error while processing batch: {}", exception.getMessage(), exception);
		}
	}
}
