package com.practikum.kafka_1.config;

import com.practikum.kafka_1.model.Order;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class KafkaListenerConfig {

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Order> batchKafkaListenerContainerFactory(
			ConsumerFactory<String, Order> consumerFactory
	) {
		ConcurrentKafkaListenerContainerFactory<String, Order> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setBatchListener(true);
		// Batch listener commits offsets only after the whole batch is processed.
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
		return factory;
	}
}
