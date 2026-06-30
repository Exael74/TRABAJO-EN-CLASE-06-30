package edu.eci.arsw.kafka.producer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventProducer {

    private static final String TOPIC = "inventory";

    private final KafkaTemplate<String, InventoryProcessedEvent> kafkaTemplate;

    public InventoryEventProducer(KafkaTemplate<String, InventoryProcessedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishInventoryProcessed(InventoryProcessedEvent event) {
        kafkaTemplate.send(TOPIC, event.getOrderId(), event);
    }
}
