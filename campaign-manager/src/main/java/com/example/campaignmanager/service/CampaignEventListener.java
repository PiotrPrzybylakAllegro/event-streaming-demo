package com.example.campaignmanager.service;

import com.example.campaignmanager.model.CampaignEvent;
import com.example.campaignmanager.model.Envelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CampaignEventListener {
    private static final Logger log = LoggerFactory.getLogger(CampaignEventListener.class);

    private final CampaignState state;
    private final ObjectMapper objectMapper;

    public CampaignEventListener(CampaignState state, ObjectMapper objectMapper) {
        this.state = state;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${app.kafka.messages-topic}", groupId = "campaign-manager-campaign-events")
    public void consume(Envelope envelope) {
        if (!Envelope.CAMPAIGN_EVENT.equals(envelope.type())) {
            return;
        }
        CampaignEvent event = objectMapper.convertValue(envelope.payload(), CampaignEvent.class);
        state.upsert(event);
        log.info("Updated campaign state: id={}, status={}", event.id(), event.status());
    }
}
