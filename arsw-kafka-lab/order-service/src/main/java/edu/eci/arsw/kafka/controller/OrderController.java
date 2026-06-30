package edu.eci.arsw.kafka.controller;

import edu.eci.arsw.kafka.dto.CreateOrderRequest;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.producer.OrderEventProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderEventProducer orderEventProducer;

    public OrderController(OrderEventProducer orderEventProducer) {
        this.orderEventProducer = orderEventProducer;
    }

    @PostMapping
    public ResponseEntity<OrderCreatedEvent> createOrder(@RequestBody CreateOrderRequest request) {
        String orderId = "ORD-" + UUID.randomUUID();
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, request.getCustomerId(), request.getTotal(), "CREATED");
        orderEventProducer.publishOrderCreated(event);
        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }
}
