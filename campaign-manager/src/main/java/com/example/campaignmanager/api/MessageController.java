package com.example.campaignmanager.api;

import com.example.campaignmanager.model.MessagePayload;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/messages")
public class MessageController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public MessageController(KafkaTemplate<String, Object> kafkaTemplate,
                             @Value("${app.kafka.messages-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @PostMapping
    public void publish(@RequestBody MessagePayload payload) {
        kafkaTemplate.send(topic, payload);
    }
}
