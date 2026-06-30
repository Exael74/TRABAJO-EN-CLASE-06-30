# Laboratorio Apache Kafka — ARSW

Sistema de pedidos basado en eventos con Apache Kafka, Spring Boot y Java 21.

## Requisitos

- Docker y Docker Compose
- Java 21
- Maven 3.8+

## Levantar infraestructura

```bash
docker compose up -d
```

Esto levanta:
- **Kafka 3.7.0** en modo KRaft (puerto `9092`)
- **Kafka UI** en `http://localhost:8080`

## Crear topics

```bash
docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 3 --replication-factor 1
docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic payments --partitions 3 --replication-factor 1
docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic inventory --partitions 3 --replication-factor 1
```

Verificar:

```bash
docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## Verificar Kafka UI

Abrir `http://localhost:8080` y confirmar que el cluster `arsw-lab` aparece con los topics creados.

## Estructura del proyecto

```
arsw-kafka-lab/
├── docker-compose.yml
├── README.md
├── docs/
│   └── propuesta-arquitectonica.md
└── order-service/
    └── ...
```

## Pruebas funcionales

### 1. Crear pedidos con distintos totales

```bash
# Total 50000 → Pago APPROVED, Inventario RESERVED
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS01","total":50000}'

# Total 200000 → Pago APPROVED, Inventario RESERVED
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS02","total":200000}'

# Total 280000 → Pago REJECTED, Inventario RESERVED
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS03","total":280000}'

# Total 400000 → Pago REJECTED, Inventario REJECTED
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS04","total":400000}'
```

Resultados esperados:

| Total | customerId | Pago (≤250k) | Inventario (≤300k) |
|---|---|---|---|
| 50000 | CUS01 | APPROVED | RESERVED |
| 200000 | CUS02 | APPROVED | RESERVED |
| 280000 | CUS03 | REJECTED | RESERVED |
| 400000 | CUS04 | REJECTED | REJECTED |

### 2. Verificar mensajes en Kafka

```bash
# Ver topics creados
docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Ver mensajes en orders (partición, offset, clave, valor)
docker exec -it arsw-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 10 \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --timeout-ms 3000
```

Kafka UI: `http://localhost:8080` → cluster `arsw-lab` → Topics → seleccionar topic → Messages.

### 3. Verificar lag por Consumer Group

```bash
docker exec -it arsw-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group payment-service --describe
```

Groups esperados: `order-service`, `payment-service`, `inventory-service`, `notification-service`, `analytics-service`.

### 4. Probar DLT (Dead Letter Topic)

Enviar un pedido con `customerId` vacío para activar el error:

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"","total":50000}'
```

El `PaymentServiceConsumer` lanza una excepción, el `DefaultErrorHandler` reintenta 3 veces (cada 2s) y luego envía el mensaje a `orders.DLT`.

Para leer el DLT:

```bash
docker exec -it arsw-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.DLT --from-beginning --max-messages 5 \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --timeout-ms 3000
```

### Script automatizado

```bash
./scripts/test-scenarios.sh
```

## Propuesta arquitectónica

(Contenido completo en `docs/propuesta-arquitectonica.md`)
