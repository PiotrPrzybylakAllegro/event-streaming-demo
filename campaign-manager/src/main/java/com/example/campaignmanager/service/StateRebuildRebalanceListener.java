package com.example.campaignmanager.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Rebalance listener for the state-rebuild consumer.  On partition assignment
 * it seeks to the beginning and records the current end offsets so that
 * {@link CampaignEventListener} knows when catch-up is complete.
 */
@Component
public class StateRebuildRebalanceListener implements ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(StateRebuildRebalanceListener.class);

    /** End offsets at assignment time — the target for catch-up. */
    private final Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        consumer.seekToBeginning(partitions);

        Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
        endOffsets.clear();
        endOffsets.putAll(ends);
        log.info("State-rebuild: assigned {} partitions, end offsets = {}", partitions.size(), ends);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        partitions.forEach(endOffsets::remove);
    }

    /**
     * Returns the end offset for a given partition, or -1 if unknown.
     */
    public long endOffsetFor(TopicPartition tp) {
        return endOffsets.getOrDefault(tp, -1L);
    }

    /**
     * True when every assigned partition's end offset is known and is 0
     * (i.e. the topic is empty), meaning catch-up is trivially done.
     */
    public boolean isTopicEmpty() {
        if (endOffsets.isEmpty()) {
            return false; // not yet assigned
        }
        return endOffsets.values().stream().allMatch(offset -> offset == 0);
    }

    /**
     * Returns true when every assigned partition with a non-zero end offset
     * is present in the given {@code caughtUpPartitions} set.  Partitions
     * with end offset 0 are trivially caught up (empty).
     */
    public boolean allCaughtUp(Set<TopicPartition> caughtUpPartitions) {
        if (endOffsets.isEmpty()) {
            return false; // not yet assigned
        }
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            if (entry.getValue() > 0 && !caughtUpPartitions.contains(entry.getKey())) {
                return false;
            }
        }
        return true;
    }
}
