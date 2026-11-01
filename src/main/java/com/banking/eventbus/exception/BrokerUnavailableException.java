package com.banking.eventbus.exception;

public class BrokerUnavailableException extends RuntimeException {
    public BrokerUnavailableException(String message) {
        super(message);
    }
}