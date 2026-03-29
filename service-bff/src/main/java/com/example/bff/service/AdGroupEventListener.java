package com.example.bff.service;

import com.example.bff.model.AdGroupEvent;
import com.example.bff.model.Envelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AdGroupEventListener {
    private static final Logger log = LoggerFactory.getLogger(AdGroupEventListener.class);

    private final Map<String, AdGroupEvent> state = new ConcurrentHashMap<>();
    private final CampaignReadModel readModel;
    private final ObjectMapper objectMapper;

    public AdGroupEventListener(CampaignReadModel readModel, ObjectMapper objectMapper) {
        this.readModel = readModel;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${app.kafka.messages-topic}", groupId = "service-bff-adgroup-events")
    public void consume(Envelope envelope) {
        if (!Envelope.ADGROUP_EVENT.equals(envelope.type())) {
            return;
        }
        AdGroupEvent event = objectMapper.convertValue(envelope.payload(), AdGroupEvent.class);
        if (event == null || event.id() == null) {
            return;
        }
        state.put(event.id(), event);
        readModel.upsert(event);
        log.info("Adgroup event stored: id={}, status={}", event.id(), event.status());
    }

    public Collection<AdGroupEvent> list() {
        return state.values();
    }
}
