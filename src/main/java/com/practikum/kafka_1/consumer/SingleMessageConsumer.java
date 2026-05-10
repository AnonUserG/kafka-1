package com.practikum.kafka_1.consumer;

import com.practikum.kafka_1.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class SingleMessageConsumer {

	@KafkaListener(
			topics = "${app.kafka.topic:my-topic}",
			groupId = "${app.kafka.single-consumer.group-id:single-message-consumer-group}",
			properties = {
					"enable.auto.commit=${app.kafka.single-consumer.enable-auto-commit:true}",
					"max.poll.records=${app.kafka.single-consumer.max-poll-records:1}"
			}
	)
	public void listen(Order order) {
		try {
			log.info("SingleMessageConsumer received order: {}", order);
		}
		catch (Exception exception) {
			log.error("SingleMessageConsumer error while processing order {}: {}", order, exception.getMessage(), exception);
		}
	}
}
