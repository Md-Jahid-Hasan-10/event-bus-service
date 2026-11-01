package com.banking.eventbus.service;

import com.banking.eventbus.dto.DeadLetterInfo;
import com.banking.eventbus.dto.TopicInfo;
import com.banking.eventbus.dto.TopicsHealth;

import java.util.List;

public interface TopicInspectionService {

    /** Is the broker reachable, and does every topic of the catalog exist? Never throws. */
    TopicsHealth health();

    /** Every topic of the catalog with its real settings, and whether they match the catalog. */
    List<TopicInfo> topics();

    /** How many messages are waiting on each dead-letter topic. */
    List<DeadLetterInfo> deadLetters();
}