package com.practikum.kafka_1;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "app.kafka.enabled=false")
class Kafka1ApplicationTests {

	@Test
	void contextLoads() {
	}

}
