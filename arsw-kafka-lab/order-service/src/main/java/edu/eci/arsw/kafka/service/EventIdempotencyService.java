package edu.eci.arsw.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class EventIdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(EventIdempotencyService.class);

    private final Set<String> processedEvents = ConcurrentHashMap.newKeySet();

    public boolean isProcessed(String eventId) {
        return processedEvents.contains(eventId);
    }

    public void markProcessed(String eventId) {
        processedEvents.add(eventId);
        log.trace("Event {} marked as processed", eventId);
    }
}
