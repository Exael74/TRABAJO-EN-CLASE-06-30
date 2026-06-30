package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.producer.InventoryEventProducer;
import edu.eci.arsw.kafka.service.EventIdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class InventoryServiceConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryServiceConsumer.class);

    private final InventoryEventProducer inventoryEventProducer;
    private final EventIdempotencyService idempotencyService;

    public InventoryServiceConsumer(InventoryEventProducer inventoryEventProducer, EventIdempotencyService idempotencyService) {
        this.inventoryEventProducer = inventoryEventProducer;
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(topics = "orders", groupId = "inventory-service")
    public void consumeOrderCreated(OrderCreatedEvent event) {
        if (idempotencyService.isProcessed(event.getEventId())) {
            log.info("InventoryService skipping already processed event: {}", event.getEventId());
            return;
        }
        log.info("InventoryService received order: {}", event.getOrderId());

        String status;
        if (event.getTotal() <= 300000) {
            status = "RESERVED";
        } else {
            status = "REJECTED";
        }

        String inventoryId = "INV-" + UUID.randomUUID();
        InventoryProcessedEvent inventoryEvent = new InventoryProcessedEvent(
                inventoryId, event.getOrderId(), event.getCustomerId(), status);

        inventoryEventProducer.publishInventoryProcessed(inventoryEvent);
        idempotencyService.markProcessed(event.getEventId());
        log.info("InventoryService published {} for order {}", status, event.getOrderId());
    }
}
