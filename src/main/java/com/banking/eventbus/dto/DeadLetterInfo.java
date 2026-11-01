package com.banking.eventbus.dto;

public record DeadLetterInfo(String topic, long messages) {}