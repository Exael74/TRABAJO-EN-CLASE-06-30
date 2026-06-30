package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import edu.eci.arsw.kafka.service.EventIdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationServiceConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationServiceConsumer.class);

    private final EventIdempotencyService idempotencyService;

    public NotificationServiceConsumer(EventIdempotencyService idempotencyService) {
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(topics = "payments", groupId = "notification-service")
    public void consumePaymentEvent(PaymentProcessedEvent event) {
        if (idempotencyService.isProcessed(event.getEventId())) {
            log.info("NotificationService skipping already processed payment event: {}", event.getEventId());
            return;
        }
        log.info("NOTIFICATION: Payment {} for order {} - Status: {}",
                event.getPaymentId(), event.getOrderId(), event.getStatus());
        idempotencyService.markProcessed(event.getEventId());
    }

    @KafkaListener(topics = "inventory", groupId = "notification-service")
    public void consumeInventoryEvent(InventoryProcessedEvent event) {
        if (idempotencyService.isProcessed(event.getEventId())) {
            log.info("NotificationService skipping already processed inventory event: {}", event.getEventId());
            return;
        }
        log.info("NOTIFICATION: Inventory {} for order {} - Status: {}",
                event.getInventoryId(), event.getOrderId(), event.getStatus());
        idempotencyService.markProcessed(event.getEventId());
    }
}
