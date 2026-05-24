package com.practikum.kafka_1.model;

import java.time.Instant;

public record ChatMessage(
        String id,
        String senderId,
        String recipientId,
        String text,
        Instant timestamp
) {}
