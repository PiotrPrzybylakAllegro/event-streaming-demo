package com.example.bff.api;

import com.example.bff.model.AdGroupEvent;
import com.example.bff.service.AdGroupEventListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@RestController
@RequestMapping("/adgroups")
public class AdGroupController {

    private final AdGroupEventListener adGroupEventListener;

    public AdGroupController(AdGroupEventListener adGroupEventListener) {
        this.adGroupEventListener = adGroupEventListener;
    }

    @GetMapping
    public Collection<AdGroupEvent> list() {
        return adGroupEventListener.list();
    }
}
