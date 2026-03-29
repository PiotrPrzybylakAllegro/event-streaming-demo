package com.example.campaignmanager.model;

public record Envelope(String type, Object payload) {

    public static final String CAMPAIGN_COMMAND = "campaign-command";
    public static final String ADGROUP_COMMAND = "adgroup-command";
    public static final String CAMPAIGN_EVENT = "campaign-event";
    public static final String ADGROUP_EVENT = "adgroup-event";

    public static Envelope campaignCommand(CreateCampaignCommand command) {
        return new Envelope(CAMPAIGN_COMMAND, command);
    }

    public static Envelope adGroupCommand(CreateAdGroupCommand command) {
        return new Envelope(ADGROUP_COMMAND, command);
    }

    public static Envelope campaignEvent(CampaignEvent event) {
        return new Envelope(CAMPAIGN_EVENT, event);
    }

    public static Envelope adGroupEvent(AdGroupEvent event) {
        return new Envelope(ADGROUP_EVENT, event);
    }
}
