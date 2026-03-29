package com.example.servicea.service;

import com.example.servicea.model.AdGroupEvent;
import com.example.servicea.model.CreateAdGroupCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class AdGroupCommandListener {
    private static final Logger log = LoggerFactory.getLogger(AdGroupCommandListener.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final CampaignState campaignState;
    private final String eventTopic;

    public AdGroupCommandListener(KafkaTemplate<String, Object> kafkaTemplate,
                                  CampaignState campaignState,
                                  @Value("${app.kafka.adgroup-event-topic}") String eventTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.campaignState = campaignState;
        this.eventTopic = eventTopic;
    }

    @KafkaListener(topics = "${app.kafka.adgroup-topic}", containerFactory = "adGroupListenerContainerFactory")
    public void consume(CreateAdGroupCommand command) {
        AdGroupEvent event = validateAndBuildEvent(command);
        kafkaTemplate.send(eventTopic, event.id(), event);
        log.info("Emitted adgroup event: id={}, status={}, reason={}", event.id(), event.status(), event.reason());
    }

    private AdGroupEvent validateAndBuildEvent(CreateAdGroupCommand command) {
        String id = command != null ? command.id() : null;
        String campaignId = command != null ? command.campaignId() : null;
        String name = command != null ? command.name() : null;
        Double budget = command != null ? command.budget() : null;

        if (isBlank(id)) {
            return new AdGroupEvent(fallbackId(id), safe(campaignId), safe(name), safe(budget), "REJECTED", "id is required");
        }
        if (isBlank(campaignId)) {
            return new AdGroupEvent(id, safe(campaignId), safe(name), safe(budget), "REJECTED", "campaignId is required");
        }
        if (!campaignState.exists(campaignId)) {
            return new AdGroupEvent(id, campaignId, safe(name), safe(budget), "REJECTED", "campaign not found");
        }
        if (isBlank(name)) {
            return new AdGroupEvent(id, campaignId, safe(name), safe(budget), "REJECTED", "name is required");
        }
        if (budget == null || budget <= 0) {
            return new AdGroupEvent(id, campaignId, name, budget != null ? budget : 0, "REJECTED", "budget must be greater than zero");
        }
        return new AdGroupEvent(id, campaignId, name, budget, "APPROVED", null);
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
