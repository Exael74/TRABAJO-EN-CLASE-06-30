package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import edu.eci.arsw.kafka.producer.PaymentEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class PaymentServiceConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentServiceConsumer.class);

    private final PaymentEventProducer paymentEventProducer;

    public PaymentServiceConsumer(PaymentEventProducer paymentEventProducer) {
        this.paymentEventProducer = paymentEventProducer;
    }

    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consumeOrderCreated(OrderCreatedEvent event) {
        log.info("PaymentService received order: {}", event.getOrderId());

        String status;
        if (event.getTotal() <= 250000) {
            status = "APPROVED";
        } else {
            status = "REJECTED";
        }

        String paymentId = "PAY-" + UUID.randomUUID();
        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                paymentId, event.getOrderId(), event.getCustomerId(), event.getTotal(), status);

        paymentEventProducer.publishPaymentProcessed(paymentEvent);
        log.info("PaymentService published {} for order {}", status, event.getOrderId());
    }
}
