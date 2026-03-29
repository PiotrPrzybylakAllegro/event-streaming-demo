package com.example.servicea.service;

import com.example.servicea.model.CampaignEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CampaignEventListener {
    private static final Logger log = LoggerFactory.getLogger(CampaignEventListener.class);

    private final CampaignState state;

    public CampaignEventListener(CampaignState state) {
        this.state = state;
    }

    @KafkaListener(topics = "${app.kafka.campaign-event-topic}", containerFactory = "campaignEventListenerContainerFactory")
    public void consume(CampaignEvent event) {
        state.upsert(event);
        log.info("Updated campaign state: id={}, status={}", event.id(), event.status());
    }
}
