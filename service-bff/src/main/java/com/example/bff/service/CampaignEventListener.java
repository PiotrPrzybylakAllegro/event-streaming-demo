package com.example.bff.service;

import com.example.bff.model.CampaignEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CampaignEventListener {
    private static final Logger log = LoggerFactory.getLogger(CampaignEventListener.class);

    private final CampaignReadModel readModel;

    public CampaignEventListener(CampaignReadModel readModel) {
        this.readModel = readModel;
    }

    @KafkaListener(topics = "${app.kafka.event-topic}")
    public void consume(CampaignEvent event) {
        readModel.upsert(event);
        log.info("Updated campaign read model: id={}, status={}", event.id(), event.status());
    }
}
