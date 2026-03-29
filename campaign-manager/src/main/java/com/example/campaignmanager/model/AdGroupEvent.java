package com.example.campaignmanager.model;

public record AdGroupEvent(String id, String campaignId, String name, double budget, String status, String reason) { }
