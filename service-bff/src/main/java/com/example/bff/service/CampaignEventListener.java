package com.example.bff.service;

import com.example.bff.model.CampaignEvent;
import com.example.bff.model.Envelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CampaignEventListener {
    private static final Logger log = LoggerFactory.getLogger(CampaignEventListener.class);

    private final CampaignReadModel readModel;
    private final ObjectMapper objectMapper;

    public CampaignEventListener(CampaignReadModel readModel, ObjectMapper objectMapper) {
        this.readModel = readModel;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${app.kafka.messages-topic}", groupId = "service-bff-campaign-events")
    public void consume(Envelope envelope) {
        if (!Envelope.CAMPAIGN_EVENT.equals(envelope.type())) {
            return;
        }
        CampaignEvent event = objectMapper.convertValue(envelope.payload(), CampaignEvent.class);
        readModel.upsert(event);
        log.info("Updated campaign read model: id={}, status={}", event.id(), event.status());
    }
}
