package com.example.campaignmanager.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Gates command-processing listeners until the state-rebuild consumer has
 * caught up to the log end.  On startup the command listener containers are
 * configured with {@code autoStartup = false}.  Once the state-rebuild
 * consumer reaches the end offsets that existed at assignment time,
 * {@link #markReady()} starts them.
 */
@Component
public class ReplayGate {

    private static final Logger log = LoggerFactory.getLogger(ReplayGate.class);

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final KafkaListenerEndpointRegistry registry;

    public ReplayGate(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public boolean isReady() {
        return ready.get();
    }

    /**
     * Called by the state-rebuild listener once it has consumed up to the
     * end offsets recorded at partition-assignment time.
     */
    public void markReady() {
        if (ready.compareAndSet(false, true)) {
            log.info("State rebuild complete – starting command listener containers");
            startContainer("campaignCommandListener");
            startContainer("adGroupCommandListener");
        }
    }

    private void startContainer(String id) {
        var container = registry.getListenerContainer(id);
        if (container != null && !container.isRunning()) {
            container.start();
            log.info("Started listener container: {}", id);
        }
    }
}
