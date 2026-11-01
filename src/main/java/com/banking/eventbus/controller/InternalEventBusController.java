package com.banking.eventbus.controller;

import com.banking.eventbus.dto.DeadLetterInfo;
import com.banking.eventbus.dto.TopicInfo;
import com.banking.eventbus.dto.TopicsHealth;
import com.banking.eventbus.service.TopicInspectionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/internal/events")
public class InternalEventBusController {

    private final TopicInspectionService inspectionService;

    public InternalEventBusController(TopicInspectionService inspectionService) {
        this.inspectionService = inspectionService;
    }

    @GetMapping("/health")
    public ResponseEntity<TopicsHealth> health() {
        TopicsHealth health = inspectionService.health();
        HttpStatus status = health.ready() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(status).body(health);
    }

    @GetMapping("/topics")
    public ResponseEntity<List<TopicInfo>> topics() {
        return ResponseEntity.ok(inspectionService.topics());
    }

    @GetMapping("/dead-letters")
    public ResponseEntity<List<DeadLetterInfo>> deadLetters() {
        return ResponseEntity.ok(inspectionService.deadLetters());
    }
}