package edu.eci.arsw.kafka.producer;

import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventProducer {

    private static final String TOPIC = "payments";

    private final KafkaTemplate<String, PaymentProcessedEvent> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<String, PaymentProcessedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentProcessed(PaymentProcessedEvent event) {
        kafkaTemplate.send(TOPIC, event.getOrderId(), event);
    }
}
