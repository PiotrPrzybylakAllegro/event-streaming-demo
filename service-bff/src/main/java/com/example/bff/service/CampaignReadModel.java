package com.example.bff.service;

import com.example.bff.model.CampaignEvent;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CampaignReadModel {

    private final Map<String, CampaignEvent> state = new ConcurrentHashMap<>();

    public void upsert(CampaignEvent event) {
        if (event == null || event.id() == null) {
            return;
        }
        state.put(event.id(), event);
    }

    public Collection<CampaignEvent> list() {
        return state.values();
    }

    public boolean exists(String campaignId) {
        return campaignId != null && state.containsKey(campaignId);
    }
}
