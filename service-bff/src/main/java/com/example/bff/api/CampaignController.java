package com.example.bff.api;

import com.example.bff.model.CreateCampaignCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/campaigns")
public class CampaignController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public CampaignController(KafkaTemplate<String, Object> kafkaTemplate,
                              @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @PostMapping
    public void createCampaign(@RequestBody CreateCampaignCommand command) {
        kafkaTemplate.send(topic, command.id(), command);
    }
}
