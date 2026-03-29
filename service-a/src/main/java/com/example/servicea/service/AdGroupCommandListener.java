package com.example.servicea.service;

import com.example.servicea.model.AdGroupEvent;
import com.example.servicea.model.CreateAdGroupCommand;
import com.example.servicea.model.Envelope;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final String topic;
    private final ObjectMapper objectMapper;

    public AdGroupCommandListener(KafkaTemplate<String, Object> kafkaTemplate,
                                  CampaignState campaignState,
                                  ObjectMapper objectMapper,
                                  @Value("${app.kafka.messages-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.campaignState = campaignState;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @KafkaListener(topics = "${app.kafka.messages-topic}", groupId = "service-a-adgroup-commands")
    public void consume(Envelope envelope) {
        if (!Envelope.ADGROUP_COMMAND.equals(envelope.type())) {
            return;
        }
        CreateAdGroupCommand command = objectMapper.convertValue(envelope.payload(), CreateAdGroupCommand.class);
        AdGroupEvent event = validateAndBuildEvent(command);
        kafkaTemplate.send(topic, event.id(), Envelope.adGroupEvent(event));
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
