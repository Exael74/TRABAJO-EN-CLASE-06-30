package edu.eci.arsw.kafka.dto;

import java.time.Instant;
import java.util.UUID;

public class PaymentProcessedEvent {

    private String eventId;
    private String paymentId;
    private String orderId;
    private String customerId;
    private double total;
    private String status;
    private Instant occurredAt;

    public PaymentProcessedEvent() {
    }

    public PaymentProcessedEvent(String paymentId, String orderId, String customerId, double total, String status) {
        this.eventId = UUID.randomUUID().toString();
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.total = total;
        this.status = status;
        this.occurredAt = Instant.now();
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(String paymentId) {
        this.paymentId = paymentId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public double getTotal() {
        return total;
    }

    public void setTotal(double total) {
        this.total = total;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }
}
