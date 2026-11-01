package com.banking.eventbus.config;

import com.banking.eventbus.catalog.TopicCatalog;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicsConfig {

    @Bean
    public KafkaAdmin.NewTopics bankingTopics(TopicCatalog catalog,
                                              @Value("${app.kafka.replication-factor:1}") int replicationFactor) {
        NewTopic[] topics = catalog.all().stream()
                .map(definition -> TopicBuilder.name(definition.name())
                        .partitions(definition.partitions())
                        .replicas(replicationFactor)
                        .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(definition.retentionMs()))
                        .build())
                .toArray(NewTopic[]::new);

        return new KafkaAdmin.NewTopics(topics);
    }
}