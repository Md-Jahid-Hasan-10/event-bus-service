package com.banking.eventbus.dto;

public record TopicInfo(
        String name,
        String kind,
        boolean exists,
        int partitions,
        Long retentionMs,
        boolean inSync,
        String description
) {}