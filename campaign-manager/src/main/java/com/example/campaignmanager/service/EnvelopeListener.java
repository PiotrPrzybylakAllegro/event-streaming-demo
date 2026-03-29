package com.example.campaignmanager.service;

import com.example.campaignmanager.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single Kafka listener for the campaign-manager service.  All messages on
 * the shared {@code ad-manager-events} topic are received here in partition
 * order and routed by {@link Envelope#type()}.
 *
 * <p>The campaign-manager owns command processing: it consumes commands,
 * validates them, updates internal {@link CampaignState}, and publishes
 * the resulting events.  It never listens to events — they are its own
 * output.</p>
 *
 * <h3>Replay gate</h3>
 * On startup the consumer seeks to offset 0 (via
 * {@link StateRebuildRebalanceListener}) and replays the entire log.
 * During replay, commands are re-validated to rebuild {@link CampaignState}
 * but the resulting events are <em>not</em> published (they already exist
 * on the topic from the original processing).  Events are skipped entirely.
 * Once caught up, the {@link ReplayGate} opens and new commands are
 * processed normally (with event publication).
 */
@Service
public class EnvelopeListener {

    private static final Logger log = LoggerFactory.getLogger(EnvelopeListener.class);

    private final CampaignState campaignState;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;
    private final ReplayGate replayGate;
    private final StateRebuildRebalanceListener rebalanceListener;

    /** Partitions whose catch-up is confirmed (offset + 1 >= endOffset). */
    private final Set<TopicPartition> caughtUpPartitions = ConcurrentHashMap.newKeySet();

    public EnvelopeListener(CampaignState campaignState,
                            ObjectMapper objectMapper,
                            KafkaTemplate<String, Object> kafkaTemplate,
                            @Value("${app.kafka.messages-topic}") String topic,
                            ReplayGate replayGate,
                            StateRebuildRebalanceListener rebalanceListener) {
        this.campaignState = campaignState;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.replayGate = replayGate;
        this.rebalanceListener = rebalanceListener;
    }

    /* ------------------------------------------------------------------ */
    /*  Kafka listener                                                      */
    /* ------------------------------------------------------------------ */

    @KafkaListener(
            topics = "${app.kafka.messages-topic}",
            groupId = "campaign-manager",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, Envelope> record) {
        Envelope envelope = record.value();

        if (!replayGate.isReady()) {
            // Replay phase: re-process commands to rebuild state, skip events
            replayCommand(envelope);
            checkCaughtUp(record);
            return;
        }

        // Normal phase: handle commands (ignore events — they are our own output)
        switch (envelope.type()) {
            case Envelope.CAMPAIGN_COMMAND -> handleCampaignCommand(envelope);
            case Envelope.ADGROUP_COMMAND  -> handleAdGroupCommand(envelope);
            default -> {} // ignore events and unknown types
        }
    }

    /**
     * Empty-topic edge case: if the topic has no records the listener is
     * never called, so catch-up never fires.  The idle event signals that
     * no records were available, allowing us to open the gate.
     */
    @EventListener
    public void onIdle(ListenerContainerIdleEvent event) {
        if (replayGate.isReady()) {
            return;
        }
        if (rebalanceListener.isTopicEmpty()) {
            log.info("Consumer idle and topic is empty – opening replay gate");
            replayGate.markReady();
        }
    }

    /* ------------------------------------------------------------------ */
    /*  Replay: rebuild state from commands without publishing events        */
    /* ------------------------------------------------------------------ */

    private void replayCommand(Envelope envelope) {
        switch (envelope.type()) {
            case Envelope.CAMPAIGN_COMMAND -> {
                CreateCampaignCommand cmd = objectMapper.convertValue(envelope.payload(), CreateCampaignCommand.class);
                CampaignEvent event = validateCampaignCommand(cmd);
                campaignState.upsert(event);
                log.debug("Replay: rebuilt campaign state from command: id={}, status={}", event.id(), event.status());
            }
            case Envelope.ADGROUP_COMMAND -> {
                // Ad group commands don't affect CampaignState, nothing to rebuild
                log.debug("Replay: skipped adgroup command (no state to rebuild)");
            }
            default -> {} // skip events during replay
        }
    }

    /* ------------------------------------------------------------------ */
    /*  Normal command handling: validate, update state, publish event       */
    /* ------------------------------------------------------------------ */

    private void handleCampaignCommand(Envelope envelope) {
        CreateCampaignCommand command = objectMapper.convertValue(envelope.payload(), CreateCampaignCommand.class);
        CampaignEvent event = validateCampaignCommand(command);
        campaignState.upsert(event);
        kafkaTemplate.send(topic, event.id(), Envelope.campaignEvent(event));
        log.info("Emitted campaign event: id={}, status={}, reason={}", event.id(), event.status(), event.reason());
    }

    private void handleAdGroupCommand(Envelope envelope) {
        CreateAdGroupCommand command = objectMapper.convertValue(envelope.payload(), CreateAdGroupCommand.class);
        AdGroupEvent event = validateAdGroupCommand(command);
        kafkaTemplate.send(topic, event.id(), Envelope.adGroupEvent(event));
        log.info("Emitted adgroup event: id={}, status={}, reason={}", event.id(), event.status(), event.reason());
    }

    /* ------------------------------------------------------------------ */
    /*  Campaign command validation                                         */
    /* ------------------------------------------------------------------ */

    private CampaignEvent validateCampaignCommand(CreateCampaignCommand command) {
        String id = command != null ? command.id() : null;
        String name = command != null ? command.name() : null;
        Double budget = command != null ? command.budget() : null;

        if (isBlank(id)) {
            return new CampaignEvent(fallbackId(id), safe(name), safe(budget), "REJECTED", "id is required");
        }
        if (isBlank(name)) {
            return new CampaignEvent(id, safe(name), safe(budget), "REJECTED", "name is required");
        }
        if (budget == null || budget <= 0) {
            return new CampaignEvent(id, name, budget != null ? budget : 0, "REJECTED", "budget must be greater than zero");
        }
        return new CampaignEvent(id, name, budget, "APPROVED", null);
    }

    /* ------------------------------------------------------------------ */
    /*  Ad group command validation                                         */
    /* ------------------------------------------------------------------ */

    private AdGroupEvent validateAdGroupCommand(CreateAdGroupCommand command) {
        String id = command != null ? command.id() : null;
        String campaignId = command != null ? command.campaignId() : null;
        String name = command != null ? command.name() : null;
        Double budget = command != null ? command.budget() : null;

        if (isBlank(id)) {
            return new AdGroupEvent(fallbackId(id), safe(campaignId), safe(name), safe(budget), "REJECTED", "id is required");
        }
        if (isBlank(campaignId)) {
            return new AdGroupEvent(id, safe(campaignId), safe(name), safe(budget), "REJECTED", "campaignId is required");
        }
        if (!campaignState.exists(campaignId)) {
            return new AdGroupEvent(id, campaignId, safe(name), safe(budget), "REJECTED", "campaign not found");
        }
        if (isBlank(name)) {
            return new AdGroupEvent(id, campaignId, safe(name), safe(budget), "REJECTED", "name is required");
        }
        if (budget == null || budget <= 0) {
            return new AdGroupEvent(id, campaignId, name, budget != null ? budget : 0, "REJECTED", "budget must be greater than zero");
        }
        return new AdGroupEvent(id, campaignId, name, budget, "APPROVED", null);
    }

    /* ------------------------------------------------------------------ */
    /*  Catch-up detection                                                  */
    /* ------------------------------------------------------------------ */

    private void checkCaughtUp(ConsumerRecord<String, Envelope> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        long endOffset = rebalanceListener.endOffsetFor(tp);

        if (endOffset < 0) {
            return;
        }

        if (record.offset() + 1 >= endOffset) {
            caughtUpPartitions.add(tp);
            log.debug("Partition {} caught up (offset={}, endOffset={})", tp, record.offset(), endOffset);

            if (rebalanceListener.allCaughtUp(caughtUpPartitions)) {
                log.info("All partitions caught up – opening replay gate");
                replayGate.markReady();
            }
        }
    }

    /* ------------------------------------------------------------------ */
    /*  Utility methods                                                     */
    /* ------------------------------------------------------------------ */

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private String fallbackId(String id) {
        return isBlank(id) ? UUID.randomUUID().toString() : id;
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    private double safe(Double value) {
        return value == null ? 0 : value;
    }
}
