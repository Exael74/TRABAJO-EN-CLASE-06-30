package edu.eci.arsw.kafka.dto;

import java.time.Instant;
import java.util.UUID;

public class InventoryProcessedEvent {

    private String eventId;
    private String inventoryId;
    private String orderId;
    private String customerId;
    private String status;
    private Instant occurredAt;

    public InventoryProcessedEvent() {
    }

    public InventoryProcessedEvent(String inventoryId, String orderId, String customerId, String status) {
        this.eventId = UUID.randomUUID().toString();
        this.inventoryId = inventoryId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.status = status;
        this.occurredAt = Instant.now();
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getInventoryId() {
        return inventoryId;
    }

    public void setInventoryId(String inventoryId) {
        this.inventoryId = inventoryId;
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
