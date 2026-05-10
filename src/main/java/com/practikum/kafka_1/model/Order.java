package com.practikum.kafka_1.model;

import java.time.Instant;
import java.util.UUID;

public record Order(UUID id, String username, Instant createdAt) {
}
