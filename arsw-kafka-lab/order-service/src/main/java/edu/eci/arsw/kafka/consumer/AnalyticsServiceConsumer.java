package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import edu.eci.arsw.kafka.service.EventIdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class AnalyticsServiceConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsServiceConsumer.class);

    private final EventIdempotencyService idempotencyService;
    private final Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();

    public AnalyticsServiceConsumer(EventIdempotencyService idempotencyService) {
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(topics = "orders", groupId = "analytics-service")
    public void consumeOrderCreated(OrderCreatedEvent event) {
        if (idempotencyService.isProcessed(event.getEventId())) {
            log.info("AnalyticsService skipping already processed order event: {}", event.getEventId());
            return;
        }
        counters.computeIfAbsent("orders", k -> new AtomicInteger(0)).incrementAndGet();
        log.info("ANALYTICS: Order created - {} | Total orders: {}",
                event.getOrderId(), counters.get("orders").get());
        idempotencyService.markProcessed(event.getEventId());
    }

    @KafkaListener(topics = "payments", groupId = "analytics-service")
    public void consumePaymentEvent(PaymentProcessedEvent event) {
        if (idempotencyService.isProcessed(event.getEventId())) {
            log.info("AnalyticsService skipping already processed payment event: {}", event.getEventId());
            return;
        }
        counters.computeIfAbsent("payments", k -> new AtomicInteger(0)).incrementAndGet();
        log.info("ANALYTICS: Payment {} for order {} - {} | Total payments: {}",
                event.getPaymentId(), event.getOrderId(), event.getStatus(),
                counters.get("payments").get());
        idempotencyService.markProcessed(event.getEventId());
    }

    @KafkaListener(topics = "inventory", groupId = "analytics-service")
    public void consumeInventoryEvent(InventoryProcessedEvent event) {
        if (idempotencyService.isProcessed(event.getEventId())) {
            log.info("AnalyticsService skipping already processed inventory event: {}", event.getEventId());
            return;
        }
        counters.computeIfAbsent("inventory", k -> new AtomicInteger(0)).incrementAndGet();
        log.info("ANALYTICS: Inventory {} for order {} - {} | Total inventory events: {}",
                event.getInventoryId(), event.getOrderId(), event.getStatus(),
                counters.get("inventory").get());
        idempotencyService.markProcessed(event.getEventId());
    }
}
