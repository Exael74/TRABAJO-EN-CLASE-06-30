package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationServiceConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationServiceConsumer.class);

    @KafkaListener(topics = "payments", groupId = "notification-service")
    public void consumePaymentEvent(PaymentProcessedEvent event) {
        log.info("NOTIFICATION: Payment {} for order {} - Status: {}",
                event.getPaymentId(), event.getOrderId(), event.getStatus());
    }

    @KafkaListener(topics = "inventory", groupId = "notification-service")
    public void consumeInventoryEvent(InventoryProcessedEvent event) {
        log.info("NOTIFICATION: Inventory {} for order {} - Status: {}",
                event.getInventoryId(), event.getOrderId(), event.getStatus());
    }
}
