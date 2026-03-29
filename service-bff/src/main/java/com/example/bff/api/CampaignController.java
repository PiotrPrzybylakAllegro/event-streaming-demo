package com.example.bff.api;

import com.example.bff.model.CampaignEvent;
import com.example.bff.model.CreateCampaignCommand;
import com.example.bff.model.CreateAdGroupCommand;
import com.example.bff.service.CampaignReadModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@RestController
@RequestMapping("/campaigns")
public class CampaignController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;
    private final String adGroupTopic;
    private final CampaignReadModel readModel;

    public CampaignController(KafkaTemplate<String, Object> kafkaTemplate,
                              CampaignReadModel readModel,
                              @Value("${app.kafka.topic}") String topic,
                              @Value("${app.kafka.adgroup-topic}") String adGroupTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.adGroupTopic = adGroupTopic;
        this.readModel = readModel;
    }

    @PostMapping
    public void createCampaign(@RequestBody CreateCampaignCommand command) {
        kafkaTemplate.send(topic, command.id(), command);
    }

    @GetMapping
    public Collection<CampaignEvent> listCampaigns() {
        return readModel.list();
    }

    @PostMapping("/{campaignId}/adgroups")
    public void createAdGroup(@PathVariable("campaignId") String campaignId,
                              @RequestBody CreateAdGroupCommand command) {
        CreateAdGroupCommand enriched = new CreateAdGroupCommand(
                command.id(),
                campaignId,
                command.name(),
                command.budget(),
                command.startDate(),
                command.endDate()
        );
        kafkaTemplate.send(adGroupTopic, enriched.id(), enriched);
    }
}
