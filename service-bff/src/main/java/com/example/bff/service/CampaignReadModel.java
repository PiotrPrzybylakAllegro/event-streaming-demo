package com.example.bff.service;

import com.example.bff.model.AdGroupEvent;
import com.example.bff.model.CampaignEvent;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class CampaignReadModel {

    private final Map<String, CampaignEvent> campaigns = new ConcurrentHashMap<>();
    private final Map<String, Map<String, AdGroupEvent>> adGroupsByCampaign = new ConcurrentHashMap<>();

    public void upsert(CampaignEvent event) {
        if (event == null || event.id() == null) {
            return;
        }
        campaigns.put(event.id(), event);
    }

    public void upsert(AdGroupEvent event) {
        if (event == null || event.id() == null || event.campaignId() == null) {
            return;
        }
        adGroupsByCampaign
                .computeIfAbsent(event.campaignId(), ignored -> new ConcurrentHashMap<>())
                .put(event.id(), event);
    }

    public Collection<CampaignWithAdGroups> list() {
        return campaigns.values().stream()
                .map(campaign -> new CampaignWithAdGroups(
                        campaign.id(),
                        campaign.name(),
                        campaign.budget(),
                        campaign.status(),
                        campaign.reason(),
                        adGroupsByCampaign.getOrDefault(campaign.id(), Map.of()).values()
                ))
                .collect(Collectors.toList());
    }

    public boolean exists(String campaignId) {
        return campaignId != null && campaigns.containsKey(campaignId);
    }

    public Collection<AdGroupEvent> listAdGroups() {
        return adGroupsByCampaign.values().stream()
                .flatMap(map -> map.values().stream())
                .collect(Collectors.toCollection(ArrayList::new));
    }

    public record CampaignWithAdGroups(String id, String name, double budget, String status, String reason, Collection<AdGroupEvent> adGroups) { }
}
