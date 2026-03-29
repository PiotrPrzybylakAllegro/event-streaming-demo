package com.example.campaignmanager.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Gates command processing until the state-rebuild replay has caught up.
 * During replay the single consumer processes only events; once
 * {@link #markReady()} is called, commands are also processed.
 */
@Component
public class ReplayGate {

    private static final Logger log = LoggerFactory.getLogger(ReplayGate.class);

    private final AtomicBoolean ready = new AtomicBoolean(false);

    public boolean isReady() {
        return ready.get();
    }

    /**
     * Called by the listener once all assigned partitions have been
     * replayed up to the end offsets recorded at assignment time.
     */
    public void markReady() {
        if (ready.compareAndSet(false, true)) {
            log.info("Replay gate open – command processing enabled");
        }
    }
}
