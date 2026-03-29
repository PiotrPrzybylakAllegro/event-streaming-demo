package com.example.bff.service;

import com.example.bff.model.AdGroupEvent;
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

    @KafkaListener(topics = "${app.kafka.adgroup-event-topic}", containerFactory = "adGroupListenerContainerFactory")
    public void consume(AdGroupEvent event) {
        if (event == null || event.id() == null) {
            return;
        }
        state.put(event.id(), event);
        log.info("Adgroup event stored: id={}, status={}", event.id(), event.status());
    }

    public Collection<AdGroupEvent> list() {
        return state.values();
    }
}
