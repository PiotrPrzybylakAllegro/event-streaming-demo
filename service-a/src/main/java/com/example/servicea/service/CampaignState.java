package com.example.servicea.service;

import com.example.servicea.model.CampaignEvent;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CampaignState {

    private final Map<String, CampaignEvent> state = new ConcurrentHashMap<>();

    public void upsert(CampaignEvent event) {
        if (event == null || event.id() == null) {
            return;
        }
        state.put(event.id(), event);
    }

    public boolean exists(String campaignId) {
        return campaignId != null && state.containsKey(campaignId);
    }
}
