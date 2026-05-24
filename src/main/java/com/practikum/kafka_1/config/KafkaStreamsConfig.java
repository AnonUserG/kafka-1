package com.practikum.kafka_1.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    public NewTopic messagesTopic() {
        return TopicBuilder.name("messages")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic filteredMessagesTopic() {
        return TopicBuilder.name("filtered_messages")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic blockedUsersTopic() {
        return TopicBuilder.name("blocked_users")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic bannedWordsTopic() {
        return TopicBuilder.name("banned_words")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
