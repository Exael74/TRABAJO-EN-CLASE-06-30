package edu.eci.arsw.kafka.dto;

public class CreateOrderRequest {

    private String customerId;
    private double total;

    public CreateOrderRequest() {
    }

    public CreateOrderRequest(String customerId, double total) {
        this.customerId = customerId;
        this.total = total;
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
}
