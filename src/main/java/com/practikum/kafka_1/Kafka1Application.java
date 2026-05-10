package com.practikum.kafka_1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableKafka
@EnableScheduling
public class Kafka1Application {

	public static void main(String[] args) {
		SpringApplication.run(Kafka1Application.class, args);
	}
}
