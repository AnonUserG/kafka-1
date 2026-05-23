package com.practikum.kafka_1.producer;

import com.practikum.kafka_1.model.Order;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OrderScheduler {

	private final KafkaTemplate<String, Order> orderKafkaTemplate;

	@Value("${app.kafka.topic:my-topic}")
	private String topic;

	@Scheduled(fixedDelayString = "${app.kafka.producer.fixed-delay-ms:500}")
	public void schedule() {
		UUID id = UUID.randomUUID();

		Order order = new Order(id, "anonymous", Instant.now());

		log.info("Sending order to Kafka: {}", order);

		try {
			orderKafkaTemplate.send(topic, id.toString(), order)
					.whenComplete((result, ex) -> {
						if (ex != null) {
							log.error("Failed to send order {} to topic {}: {}", id, topic, ex.getMessage(), ex);
						} else {
							log.info("Order {} sent to Kafka topic {}", id, topic);
						}
					});
		}
		catch (Exception exception) {
			log.error("Unexpected error while sending order {}: {}", id, exception.getMessage(), exception);
		}
	}
}
