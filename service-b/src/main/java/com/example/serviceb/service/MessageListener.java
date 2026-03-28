package com.example.serviceb.service;

import com.example.serviceb.model.MessagePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class MessageListener {
    private static final Logger log = LoggerFactory.getLogger(MessageListener.class);

    @KafkaListener(topics = "${app.kafka.topic}")
    public void consume(MessagePayload payload) {
        log.info("Received message: id={}, content={} from topic {}", payload.id(), payload.content(), topic);
    }

    @Value("${app.kafka.topic}")
    private String topic;
}
