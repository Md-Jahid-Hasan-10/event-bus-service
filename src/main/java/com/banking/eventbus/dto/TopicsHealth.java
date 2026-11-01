package com.banking.eventbus.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public record TopicsHealth(
        boolean brokerReachable,
        boolean allTopicsPresent,
        List<String> missingTopics
) {

    @JsonIgnore
    public boolean ready() {
        return brokerReachable && allTopicsPresent;
    }
}