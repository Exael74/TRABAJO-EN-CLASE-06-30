# Laboratorio Apache Kafka — ARSW

Sistema de pedidos basado en eventos con Apache Kafka, Spring Boot y Java 21.

Escuela Colombiana de Ingeniería Julio Garavito — Arquitecturas de Software (ARSW)

## Requisitos

- Docker y Docker Compose
- Java 21
- Maven 3.8+

## Estructura del proyecto

```
arsw-kafka-lab/
├── docker-compose.yml              # Infraestructura Kafka (KRaft) + Kafka UI
├── README.md                       # Este documento
├── docs/
│   └── propuesta-arquitectonica.md # Documento técnico + respuestas actividades
├── scripts/
│   ├── create-topics.sh            # Script para crear topics base
│   └── test-scenarios.sh           # Pruebas automatizadas
└── order-service/                  # Proyecto Spring Boot
    ├── pom.xml
    └── src/main/java/edu/eci/arsw/kafka/
        ├── OrderServiceApplication.java
        ├── config/
        │   ├── KafkaTopicConfig.java
        │   ├── KafkaErrorHandlerConfig.java
        │   └── KafkaProducerConfig.java
        ├── controller/
        │   └── OrderController.java
        ├── dto/
        │   ├── CreateOrderRequest.java
        │   ├── OrderCreatedEvent.java
        │   ├── PaymentProcessedEvent.java
        │   └── InventoryProcessedEvent.java
        ├── producer/
        │   ├── OrderEventProducer.java
        │   ├── PaymentEventProducer.java
        │   └── InventoryEventProducer.java
        ├── consumer/
        │   ├── PaymentServiceConsumer.java
        │   ├── InventoryServiceConsumer.java
        │   ├── NotificationServiceConsumer.java
        │   └── AnalyticsServiceConsumer.java
        └── service/
            └── EventIdempotencyService.java
```

## Paso 1 — Levantar infraestructura

```bash
docker compose up -d
```

Esto levanta:
- **Kafka 3.7.0** en modo KRaft (puerto `9092`)
- **Kafka UI** en `http://localhost:8080`

Verificar que ambos contenedores estén saludables:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Si `arsw-kafka` tarda en arrancar, revisar logs:

```bash
docker logs arsw-kafka --tail 20
```

## Paso 2 — Crear topics

```bash
docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 3 --replication-factor 1
docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic payments --partitions 3 --replication-factor 1
docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic inventory --partitions 3 --replication-factor 1
```

Verificar:

```bash
docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

También se crearán automáticamente los topics `orders.DLT`, `payments.DLT` e `inventory.DLT` al iniciar el servicio Spring Boot.

## Paso 3 — Iniciar el servicio Spring Boot

Desde `order-service/`:

```bash
mvn spring-boot:run
```

O, si ya compilaste:

```bash
mvn package -DskipTests
java -jar target/order-service-1.0.0.jar
```

La aplicación arrancará en `http://localhost:8081`. Confirmar en los logs que aparece `Started OrderServiceApplication`.

## Paso 4 — Pruebas funcionales

### 4.1 Crear pedidos con distintos totales

```bash
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d '{"customerId":"CUS01","total":50000}'
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d '{"customerId":"CUS02","total":200000}'
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d '{"customerId":"CUS03","total":280000}'
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d '{"customerId":"CUS04","total":400000}'
```

Resultados esperados:

| Total | customerId | Pago (≤250k) | Inventario (≤300k) |
|---|---|---|---|
| 50000 | CUS01 | APPROVED | RESERVED |
| 200000 | CUS02 | APPROVED | RESERVED |
| 280000 | CUS03 | REJECTED | RESERVED |
| 400000 | CUS04 | REJECTED | REJECTED |

### 4.2 Verificar mensajes en Kafka

**CLI:**

```bash
docker exec -it arsw-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 10 \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --timeout-ms 3000
```

**Kafka UI:** `http://localhost:8080` → cluster `arsw-lab` → Topics → seleccionar topic → Messages. Confirmar que cada mensaje tiene clave = `orderId`.

### 4.3 Verificar Consumer Groups y lag

```bash
for group in order-service payment-service inventory-service notification-service analytics-service; do
  echo "--- $group ---"
  docker exec -it arsw-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --group "$group" --describe
done
```

Cada grupo debe mostrar `CURRENT-OFFSET` ≥ 0 y `LAG` ≥ 0. Un LAG = 0 significa que no hay mensajes pendientes.

### 4.4 Ver logs de consumidores

Los logs del `order-service` muestran cada evento procesado:

```
PaymentService received order: ORD-<uuid>
PaymentService published APPROVED for order ORD-<uuid>
InventoryService received order: ORD-<uuid>
InventoryService published RESERVED for order ORD-<uuid>
NOTIFICATION: Payment PAY-<uuid> for order ORD-<uuid> - Status: APPROVED
ANALYTICS: Order created - ORD-<uuid> | Total orders: 1
```

### 4.5 Probar DLT (Dead Letter Topic)

Enviar un pedido con `customerId` vacío:

```bash
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d '{"customerId":"","total":50000}'
```

El `PaymentServiceConsumer` lanza `IllegalArgumentException`, el `DefaultErrorHandler` reintenta **3 veces (cada 2s)** y luego publica el mensaje en `orders.DLT`. En los logs debe verse el error seguido del envío a DLT.

Esperar ~10s y verificar:

```bash
docker exec -it arsw-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.DLT --from-beginning --max-messages 5 \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --timeout-ms 3000
```

### 4.6 Script automatizado

```bash
./scripts/test-scenarios.sh
```

## Solución de problemas

| Problema | Causa probable | Solución |
|---|---|---|
| `docker compose up -d` falla | Docker Desktop no está corriendo | Abrir Docker Desktop y esperar a que el motor inicie |
| Contenedor `arsw-kafka` se queda en `unhealthy` | Healthcheck usa ruta incorrecta | El healthcheck ya está corregido; si persiste, revisar `docker logs arsw-kafka` |
| No se puede conectar a Kafka desde Spring Boot | Kafka no terminó de arrancar | Esperar 30-60s adicionales antes de iniciar el servicio |
| `curl` a `localhost:8081` falla | El servicio Spring Boot no está corriendo | Ejecutar `mvn spring-boot:run` y esperar a que aparezca "Started OrderServiceApplication" |
| Consumer no recibe mensajes | Consumer Group incorrecto o offset equivocado | Verificar que `groupId` coincida y que `auto-offset-reset: earliest` esté configurado |
| Mensaje no aparece en DLT | No se lanzó excepción o los reintentos no se agotaron | Revisar logs del servicio; la validación de `customerId` vacío debe estar activa |
| En PowerShell, `curl` no funciona | `curl` es alias de `Invoke-WebRequest` | Usar `curl.exe` o `Invoke-RestMethod` |

## Verificación end-to-end (checklist)

- [ ] `docker compose up -d` levanta Kafka (KRaft) y Kafka UI sin errores
- [ ] Los topics `orders`, `payments`, `inventory` existen (3 particiones, RF=1)
- [ ] `POST /orders` publica un `OrderCreatedEvent` visible en Kafka UI con clave = `orderId`
- [ ] Existen Consumer Groups: `payment-service`, `inventory-service`, `notification-service`, `analytics-service`
- [ ] Pago: total ≤ 250000 → APPROVED; total > 250000 → REJECTED
- [ ] Inventario: total ≤ 300000 → RESERVED; total > 300000 → REJECTED
- [ ] Reintentos: 3 intentos con FixedBackOff de 2s
- [ ] DLT: mensajes fallidos aparecen en `<topic>.DLT`
- [ ] Idempotencia: `eventId` duplicados son ignorados
- [ ] `docs/propuesta-arquitectonica.md` cubre los 10 puntos del diseño + actividades teóricas

## Documento técnico

La propuesta arquitectónica completa y las respuestas a las actividades teóricas están en:

```
docs/propuesta-arquitectonica.md
```
