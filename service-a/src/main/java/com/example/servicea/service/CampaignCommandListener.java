package com.example.servicea.service;

import com.example.servicea.model.CampaignEvent;
import com.example.servicea.model.CreateCampaignCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class CampaignCommandListener {
    private static final Logger log = LoggerFactory.getLogger(CampaignCommandListener.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String eventTopic;

    public CampaignCommandListener(KafkaTemplate<String, Object> kafkaTemplate,
                                   @Value("${app.kafka.campaign-event-topic}") String eventTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventTopic = eventTopic;
    }

    @KafkaListener(topics = "${app.kafka.campaign-topic}", containerFactory = "campaignListenerContainerFactory")
    public void consume(CreateCampaignCommand command) {
        CampaignEvent event = validateAndBuildEvent(command);
        kafkaTemplate.send(eventTopic, event.id(), event);
        log.info("Emitted campaign event: id={}, status={}, reason={}", event.id(), event.status(), event.reason());
    }

    private CampaignEvent validateAndBuildEvent(CreateCampaignCommand command) {
        String id = command != null ? command.id() : null;
        String name = command != null ? command.name() : null;
        Double budget = command != null ? command.budget() : null;

        if (isBlank(id)) {
            return new CampaignEvent(fallbackId(id), safe(name), safe(budget), "REJECTED", "id is required");
        }
        if (isBlank(name)) {
            return new CampaignEvent(id, safe(name), safe(budget), "REJECTED", "name is required");
        }
        if (budget == null || budget <= 0) {
            return new CampaignEvent(id, name, budget != null ? budget : 0, "REJECTED", "budget must be greater than zero");
        }
        return new CampaignEvent(id, name, budget, "APPROVED", null);
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private String fallbackId(String id) {
        return isBlank(id) ? UUID.randomUUID().toString() : id;
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    private double safe(Double value) {
        return value == null ? 0 : value;
    }
}
