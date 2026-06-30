# Instrucciones de desarrollo para OpenCode — Laboratorio Apache Kafka (ARSW)

Este documento es un prompt/guía de instrucciones para que un agente de codificación (OpenCode) implemente de extremo a extremo el laboratorio "Apache Kafka y Arquitecturas Orientadas por Eventos" de la Escuela Colombiana de Ingeniería Julio Garavito. Úsalo como contexto/tarea inicial para el agente.

## 0. Objetivo general de la tarea

Implementar un sistema de pedidos basado en eventos con Apache Kafka (modo KRaft, sin ZooKeeper), Kafka UI, Java 21 y Spring Boot, que cubra: infraestructura con Docker Compose, productor y consumidores con Spring Kafka, flujo extendido de pagos e inventario, manejo de errores con reintentos y Dead Letter Topic (DLT), y un documento técnico final con el diseño arquitectónico (caso de estudio + reto final).

## 1. Stack y requisitos

- Docker y Docker Compose
- Java 21
- Maven 3.8+
- Spring Boot (Web, Spring for Apache Kafka)
- Apache Kafka 3.7.0 (imagen `apache/kafka:3.7.0`, modo KRaft)
- Kafka UI (`provectuslabs/kafka-ui:latest`)

## 2. Estructura del repositorio a generar

```
arsw-kafka-lab/
├── docker-compose.yml
├── README.md
├── docs/
│   └── propuesta-arquitectonica.md
└── order-service/
    ├── pom.xml
    └── src/main/java/edu/eci/arsw/kafka/
        ├── OrderServiceApplication.java
        ├── config/
        │   ├── KafkaTopicConfig.java
        │   └── KafkaErrorHandlerConfig.java
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
        └── consumer/
            ├── PaymentServiceConsumer.java
            ├── InventoryServiceConsumer.java
            ├── NotificationServiceConsumer.java
            └── AnalyticsServiceConsumer.java
└── src/main/resources/application.yml
```

Ajusta nombres si OpenCode detecta convenciones distintas, pero respeta el dominio (paquete `edu.eci.arsw.kafka`) y la separación por responsabilidad lógica de servicio (cada consumidor representa un "servicio" aunque viva en el mismo proyecto Spring Boot, tal como indica la guía).

## 3. Paso 1 — Infraestructura (Capítulo 3)

Crear `docker-compose.yml` con dos servicios:

- `kafka`: imagen `apache/kafka:3.7.0`, modo KRaft (`broker,controller`), puerto `9092`, `KAFKA_NUM_PARTITIONS: 3`, `replication-factor` 1 (entorno de laboratorio).
- `kafka-ui`: imagen `provectuslabs/kafka-ui:latest`, puerto `8080`, apuntando al cluster `kafka:9092`.

Usar exactamente la configuración de variables de entorno descrita en la guía (NODE_ID, PROCESS_ROLES, LISTENERS, ADVERTISED_LISTENERS, CONTROLLER_LISTENER_NAMES, LISTENER_SECURITY_PROTOCOL_MAP, CONTROLLER_QUORUM_VOTERS, OFFSETS_TOPIC_REPLICATION_FACTOR, TRANSACTION_STATE_LOG_REPLICATION_FACTOR, TRANSACTION_STATE_LOG_MIN_ISR, GROUP_INITIAL_REBALANCE_DELAY_MS).

Validar con:
```
docker compose up -d
docker ps
```
Kafka UI debe quedar accesible en `http://localhost:8080`.

Crear los topics obligatorios vía CLI (o documentarlo en el README): `orders`, `payments`, `inventory` (3 particiones, factor de replicación 1).

## 4. Paso 2 — Proyecto Spring Boot base (Capítulo 4)

Generar proyecto Maven Spring Boot con dependencias: `spring-boot-starter-web`, `spring-kafka`, `spring-boot-starter-test`, `spring-kafka-test`.

`application.yml`:
- `server.port: 8081`
- `spring.kafka.bootstrap-servers: localhost:9092`
- Producer: `key-serializer=StringSerializer`, `value-serializer=JsonSerializer`
- Consumer: `group-id: order-service`, `auto-offset-reset: earliest`, `key-deserializer=StringDeserializer`, `value-deserializer=JsonDeserializer`
- `spring.json.trusted.packages: edu.eci.arsw.kafka.dto`

DTOs de eventos (todos con `eventId`, datos de negocio y `occurredAt`/`Instant`):
- `OrderCreatedEvent` (orderId, customerId, total, status, occurredAt)
- `PaymentProcessedEvent` (paymentId, orderId, customerId, total, status APPROVED/REJECTED, occurredAt)
- `InventoryProcessedEvent` (inventoryId, orderId, customerId, status RESERVED/REJECTED, occurredAt)

Endpoint REST `POST /orders`:
- Recibe `CreateOrderRequest` (customerId, total)
- Construye `OrderCreatedEvent` con `orderId` generado (`ORD-` + UUID), `status=CREATED`
- Publica vía `OrderEventProducer.publishOrderCreated(event)` usando `KafkaTemplate<String, OrderCreatedEvent>` con **clave = orderId** (clave de particionamiento que conserva el orden por pedido).

Probar con:
```
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d '{"customerId":"CUS01","total":120000}'
```

## 5. Paso 3 — Flujo extendido: pagos e inventario (Capítulo 6)

Implementar dos `@KafkaListener` independientes sobre el topic `orders`, cada uno en su **propio Consumer Group** (para que ambos reciban el mismo evento sin competir por la partición):

- `PaymentServiceConsumer` — `groupId = payment-service`. Regla de negocio: aprobar si `total <= 250000`, si no, rechazar. Publica `PaymentProcessedEvent` en el topic `payments` con clave `orderId`.
- `InventoryServiceConsumer` — `groupId = inventory-service`. Regla de negocio: reservar si `total <= 300000`, si no, rechazar. Publica `InventoryProcessedEvent` en el topic `inventory` con clave `orderId`.

Agregar también (no descritos con código en la guía pero exigidos por el caso de estudio/reto final, así que deben implementarse de forma simple):
- `NotificationServiceConsumer` — `groupId = notification-service`, escucha `payments` e `inventory`, solo loguea/simula notificación.
- `AnalyticsServiceConsumer` — `groupId = analytics-service`, escucha `orders`, `payments`, `inventory`, acumula contadores simples en memoria o solo loguea.

Cada `@KafkaListener(topics = "orders", groupId = "...")` debe declararse en una clase de consumidor distinta para reflejar el diseño de Consumer Groups separados del capítulo 2.5.

## 6. Paso 4 — Manejo de errores, reintentos y DLT (Capítulo 7)

Configurar un `DefaultErrorHandler` global (bean `KafkaErrorHandlerConfig`) usando:
- `DeadLetterPublishingRecoverer` que enrute a `<topic>.DLT` (mismo particionamiento que el original)
- `FixedBackOff(2000L, 3L)` (3 reintentos cada 2s antes de enviar a DLT)

Aplicarlo a los `ConcurrentKafkaListenerContainerFactory` usados por todos los `@KafkaListener`.

Hacer los consumidores **idempotentes**: usar `eventId` (o `orderId` + tipo de evento) para evitar reprocesar el mismo evento dos veces; puede simularse con un `Set` en memoria o un registro simple, documentando que en producción debería ser una tabla/almacén persistente.

## 7. Paso 5 — Validación funcional (Actividades 3, 4, 6)

Generar al menos un script o sección de README con casos de prueba manuales:
1. Crear pedidos con distintos totales (ej: 50000, 200000, 280000, 400000) para forzar combinaciones de aprobado/rechazado en pago e inventario.
2. Verificar en Kafka UI: topic, partición, offset, clave y contenido de cada evento publicado en `orders`, `payments`, `inventory`.
3. Verificar lag por Consumer Group (`payment-service`, `inventory-service`, `notification-service`, `analytics-service`).
4. Forzar un error de consumo (por ejemplo lanzando una excepción si `customerId` es nulo o vacío) y confirmar que tras 3 reintentos el mensaje aparece en el topic `.DLT` correspondiente.

## 8. Paso 6 — Documento técnico final (Capítulos 5, 9 y 10)

Generar `docs/propuesta-arquitectonica.md` con:

1. **Resumen de la solución**: arquitectura orientada por eventos para comercio electrónico (pedidos, pagos, inventario, facturas, notificaciones, analítica, auditoría).
2. **Tabla de servicios**: order-service, payment-service, inventory-service, invoice-service, notification-service, analytics-service, audit-service, con su responsabilidad.
3. **Tabla de eventos y topics**: usar exactamente los nombres del capítulo 5.2 (`orders`, `payments`, `inventory`, `invoices`, `notifications`, `audit`) con sus eventos (`order-created`, `payment-approved`/`payment-rejected`, `inventory-reserved`/`inventory-rejected`, `invoice-generated`/`invoice-failed`, `notification-sent`/`notification-failed`, `audit-record-created`).
4. **Claves de particionamiento**: `orderId` para los topics de negocio, `correlationId` para `audit`. Justificar por qué.
5. **Consumer Groups**: uno por servicio, justificando por qué deben ser distintos (cada grupo procesa el topic de forma independiente; si compartieran grupo, las particiones se repartirían y no todos recibirían todos los eventos).
6. **Por qué no usar un único topic `events` global**: pérdida de cohesión por dominio, acoplamiento de esquemas, dificultad de escalar particiones según volumen real de cada flujo, mezcla de retenciones/SLAs distintos, mayor complejidad de filtrado en consumidores.
7. **Estrategia de errores**: tipos de error (transitorio, permanente, negocio, técnico), política de reintentos con backoff, DLT, idempotencia.
8. **Consistencia eventual**: estados del pedido (CREATED → PAYMENT_APPROVED/REJECTED, INVENTORY_RESERVED/REJECTED → CONFIRMED/CANCELLED) y aceptación de que no todo se actualiza de inmediato.
9. **Síncrono vs. asíncrono**: qué procesos siguen siendo REST (consultar catálogo, autenticación, consulta de estado) y cuáles van por Kafka (notificaciones, analítica, auditoría, integración entre pago/inventario).
10. **Riesgos y mejoras para producción**: factor de replicación > 1, monitoreo de lag, particiones según volumen, retención según necesidades de auditoría/reprocesamiento, versionado de eventos.

Este documento es el entregable exigido en el capítulo 10.2 y debe alinearse con la rúbrica (diseño de eventos 20%, topics y Consumer Groups 20%, justificación arquitectónica 25%, errores y observabilidad 15%, claridad 10%, consistencia de la propuesta 10%).

## 9. Paso 7 — Respuestas a las actividades teóricas

Incluir en `docs/` (o en el mismo `propuesta-arquitectonica.md`) respuestas breves y justificadas a:
- Actividad 1 (cap. 1): clasificación síncrono/asíncrono/híbrido de procesos de una tienda en línea.
- Actividad 2 (cap. 2): riesgos de un topic con 1 partición, replicación 1, sin clave, retención de 24h, y propuesta de mejora.
- Actividad 5 (cap. 5): diseño completo del flujo de compra (eventos, topics, productores, consumidores, Consumer Groups, claves) y justificación de por qué no usar un topic `events` único.
- Actividad 7 (cap. 7): estrategia de errores específica para `inventory-service` (cuándo reintentar, cuándo DLT, qué revisar, cómo evitar bucles infinitos).
- Actividad 8 (cap. 8): diagnóstico de una arquitectura con topic `events`, sin clave, replicación 1, sin DLT, sin monitoreo de lag — problemas, atributos de calidad afectados, mejoras prioritarias.
- Actividades del capítulo 9 (consolidación): clasificación REST/Kafka/híbrido de 8 procesos, diseño del flujo completo, y diagnóstico de la configuración propuesta en 9.3 (1 partición, replicación 1, retención 12h, sin clave, sin eventId/correlationId, un solo Consumer Group para todos, sin DLT, sin monitoreo de lag).

## 10. Orden de ejecución sugerido para el agente

1. Generar `docker-compose.yml` y levantar el entorno.
2. Crear los topics base (`orders`, `payments`, `inventory`) y validarlos en Kafka UI.
3. Scaffolding del proyecto Spring Boot (`pom.xml`, `application.yml`, paquete base).
4. Implementar DTOs y `OrderEventProducer` + `OrderController` (productor mínimo funcional).
5. Probar publicación con `curl` y verificar en Kafka UI.
6. Implementar consumidores de pago e inventario con sus reglas de negocio y productores derivados.
7. Implementar consumidores de notificación y analítica.
8. Configurar manejo de errores (reintentos + DLT) e idempotencia básica.
9. Probar el flujo de extremo a extremo con varios totales y revisar lag/DLT en Kafka UI.
10. Redactar `docs/propuesta-arquitectonica.md` con el diseño completo y las respuestas a las actividades.
11. Completar `README.md` con instrucciones de levantamiento, pruebas y estructura del proyecto.

## 11. Criterios de aceptación (checklist final)

- [ ] `docker compose up -d` levanta Kafka (KRaft) y Kafka UI sin errores.
- [ ] Existen los topics `orders`, `payments`, `inventory` (y se documentan `invoices`, `notifications`, `audit` aunque no se implementen productores reales para todos).
- [ ] `POST /orders` publica un `OrderCreatedEvent` visible en Kafka UI con clave = `orderId`.
- [ ] Existen Consumer Groups distintos para `payment-service`, `inventory-service`, `notification-service`, `analytics-service`.
- [ ] Los eventos de pago e inventario se publican correctamente según la regla de negocio (umbral de `total`).
- [ ] Existe manejo de errores con reintentos (backoff fijo, 3 intentos) y publicación a `.DLT`.
- [ ] Los consumidores son idempotentes ante reprocesamiento del mismo evento.
- [ ] `docs/propuesta-arquitectonica.md` cubre: servicios, eventos/topics, claves, Consumer Groups, estrategia de errores, consistencia eventual, síncrono vs. asíncrono, riesgos/mejoras, y respuestas a las actividades teóricas.
- [ ] `README.md` documenta cómo levantar, probar y verificar el laboratorio completo.
