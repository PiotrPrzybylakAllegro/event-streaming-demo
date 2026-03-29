package com.example.servicea.model;

import java.time.LocalDate;

public record CreateAdGroupCommand(String id, String campaignId, String name, double budget, LocalDate startDate, LocalDate endDate) { }
