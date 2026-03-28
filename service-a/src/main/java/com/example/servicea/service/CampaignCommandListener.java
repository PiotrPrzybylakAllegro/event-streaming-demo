package com.example.servicea.service;

import com.example.servicea.model.CreateCampaignCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CampaignCommandListener {
    private static final Logger log = LoggerFactory.getLogger(CampaignCommandListener.class);

    @KafkaListener(topics = "${app.kafka.campaign-topic}")
    public void consume(CreateCampaignCommand command) {
        log.info("Received campaign command: id={}, name={}, budget={}", command.id(), command.name(), command.budget());
        // TODO: trigger domain handling / persistence as needed
    }
}
