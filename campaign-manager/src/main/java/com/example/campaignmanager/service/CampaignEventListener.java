package com.example.campaignmanager.service;

import com.example.campaignmanager.model.CampaignEvent;
import com.example.campaignmanager.model.Envelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Rebuilds in-memory {@link CampaignState} by replaying all campaign-event
 * envelopes from offset 0.  Once the consumer reaches the end offsets that
 * existed at partition-assignment time, it signals {@link ReplayGate} so
 * that command listeners can start processing new commands.
 *
 * <p>The {@link StateRebuildRebalanceListener} (wired into the container
 * factory) handles seeking to the beginning and recording end offsets.
 * This listener checks after each record whether all assigned partitions
 * have been caught up.</p>
 *
 * <p>Edge case: if the topic is empty (all end offsets are 0), the
 * {@code consume()} method is never called.  We handle that via a
 * {@link ListenerContainerIdleEvent} — when the state-rebuild container
 * goes idle and the gate is not yet open, we check whether the topic
 * is empty and open the gate.</p>
 */
@Service
public class CampaignEventListener {
    private static final Logger log = LoggerFactory.getLogger(CampaignEventListener.class);

    private final CampaignState state;
    private final ObjectMapper objectMapper;
    private final ReplayGate replayGate;
    private final StateRebuildRebalanceListener rebalanceListener;

    /** Partitions whose catch-up is confirmed (offset + 1 >= endOffset). */
    private final Set<TopicPartition> caughtUpPartitions = ConcurrentHashMap.newKeySet();

    public CampaignEventListener(CampaignState state,
                                 ObjectMapper objectMapper,
                                 ReplayGate replayGate,
                                 StateRebuildRebalanceListener rebalanceListener) {
        this.state = state;
        this.objectMapper = objectMapper;
        this.replayGate = replayGate;
        this.rebalanceListener = rebalanceListener;
    }

    @KafkaListener(
            topics = "${app.kafka.messages-topic}",
            groupId = "campaign-manager-state-rebuild",
            containerFactory = "stateRebuildListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, Envelope> record) {
        Envelope envelope = record.value();

        // Process campaign events into state
        if (Envelope.CAMPAIGN_EVENT.equals(envelope.type())) {
            CampaignEvent event = objectMapper.convertValue(envelope.payload(), CampaignEvent.class);
            state.upsert(event);
            log.debug("Rebuilt campaign state: id={}, status={}", event.id(), event.status());
        }

        // Check catch-up after each record
        if (!replayGate.isReady()) {
            checkCaughtUp(record);
        }
    }

    /**
     * Handles the empty-topic edge case.  When the state-rebuild container
     * goes idle (no records to consume), if the topic is empty and the gate
     * hasn't opened yet, we open it immediately.
     */
    @EventListener
    public void onIdle(ListenerContainerIdleEvent event) {
        if (replayGate.isReady()) {
            return;
        }
        // Only react to idle events from the state-rebuild container
        String listenerId = event.getListenerId();
        if (listenerId != null && listenerId.contains("campaign-manager-state-rebuild")) {
            if (rebalanceListener.isTopicEmpty()) {
                log.info("State-rebuild container idle and topic is empty – opening replay gate");
                replayGate.markReady();
            }
        }
    }

    private void checkCaughtUp(ConsumerRecord<String, Envelope> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        long endOffset = rebalanceListener.endOffsetFor(tp);

        if (endOffset < 0) {
            // End offset not yet recorded — shouldn't happen, but be defensive
            return;
        }

        // record.offset() is 0-based; endOffset is the next-to-be-written offset.
        // Caught up when we've processed the last record that existed at assignment time.
        if (record.offset() + 1 >= endOffset) {
            caughtUpPartitions.add(tp);
            log.debug("Partition {} caught up (offset={}, endOffset={})", tp, record.offset(), endOffset);

            // Check if ALL assigned partitions are caught up
            if (allPartitionsCaughtUp()) {
                log.info("All partitions caught up – opening replay gate");
                replayGate.markReady();
            }
        }
    }

    private boolean allPartitionsCaughtUp() {
        // We need to verify that every partition the rebalance listener knows
        // about is in our caughtUpPartitions set.  The rebalance listener
        // returns -1 for unknown partitions, so we iterate our known set.
        // A simpler check: every partition with endOffset > 0 must be caught up,
        // and partitions with endOffset == 0 are trivially caught up.
        // We rely on StateRebuildRebalanceListener.endOffsetFor() for the full set.
        // Since we don't have a direct "all partitions" accessor, we check that
        // no assigned partition is lagging.  The rebalance listener's isTopicEmpty()
        // already covers the all-zeros case; here we need to know if any partition
        // with endOffset > 0 is NOT in caughtUpPartitions.

        // Use a simple approach: ask the rebalance listener for each partition
        // we've seen — but we haven't tracked the full assignment set here.
        // Better: add a method to the rebalance listener to get all assigned partitions.
        // For now, use the fact that if isTopicEmpty() is true, gate is already open.
        // Otherwise, all partitions with records must appear in caughtUpPartitions.

        // Pragmatic: the rebalance listener has the full end-offset map.
        // Let's check against it.
        return rebalanceListener.allCaughtUp(caughtUpPartitions);
    }
}
