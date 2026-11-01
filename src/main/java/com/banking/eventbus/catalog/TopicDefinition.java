package com.banking.eventbus.catalog;

public record TopicDefinition(
        String name,
        TopicKind kind,
        int partitions,
        long retentionMs,
        String description
) {}