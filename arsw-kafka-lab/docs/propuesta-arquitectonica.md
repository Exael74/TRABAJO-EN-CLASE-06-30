# Propuesta Arquitectónica — Sistema de Pedidos Orientado por Eventos

## 1. Resumen de la solución

Arquitectura orientada por eventos (Event-Driven Architecture) para un sistema de comercio electrónico. Los pedidos se crean a través de una API REST y se publican como eventos en Apache Kafka. Los servicios de pago, inventario, notificaciones, analítica, facturación y auditoría se suscriben como consumidores independientes a los topics que les corresponden, procesando cada evento de forma asíncrona y desacoplada.

El sistema utiliza Apache Kafka 3.7.0 en modo KRaft (sin ZooKeeper), Spring Boot 3.2.5 con Java 21, y Kafka UI para monitoreo. La infraestructura se despliega con Docker Compose.

## 2. Tabla de servicios

| Servicio | Consumer Group | Topic(s) que escucha | Responsabilidad |
|---|---|---|---|
| **order-service** | `order-service` | — (productor) | Expone API REST `POST /orders`, genera `OrderCreatedEvent` y lo publica en `orders` |
| **payment-service** | `payment-service` | `orders` | Aprueba o rechaza el pago según umbral ($250.000), publica `PaymentProcessedEvent` en `payments` |
| **inventory-service** | `inventory-service` | `orders` | Reserva o rechaza inventario según umbral ($300.000), publica `InventoryProcessedEvent` en `inventory` |
| **invoice-service** | `invoice-service` | `payments` | *(Propuesto)* Escucha pagos aprobados y genera factura |
| **notification-service** | `notification-service` | `payments`, `inventory` | Simula envío de notificaciones al cliente (log) |
| **analytics-service** | `analytics-service` | `orders`, `payments`, `inventory` | Acumula contadores en memoria para métricas de negocio (log + `ConcurrentHashMap`) |
| **audit-service** | `audit-service` | `orders`, `payments`, `inventory`, `invoices`, `notifications` | *(Propuesto)* Registra todos los eventos para auditoría |

Los servicios `invoice-service` y `audit-service` están documentados aquí como parte del diseño arquitectónico completo, aunque no se implementaron en el código del laboratorio.

## 3. Tabla de eventos y topics

| Topic | Evento | Productor | Descripción |
|---|---|---|---|
| **orders** | `order-created` | `OrderEventProducer` | Se crea un nuevo pedido |
| **payments** | `payment-approved` | `PaymentEventProducer` | Pago aprobado (total ≤ $250.000) |
| **payments** | `payment-rejected` | `PaymentEventProducer` | Pago rechazado (total > $250.000) |
| **inventory** | `inventory-reserved` | `InventoryEventProducer` | Inventario reservado (total ≤ $300.000) |
| **inventory** | `inventory-rejected` | `InventoryEventProducer` | Inventario rechazado (total > $300.000) |
| **invoices** | `invoice-generated` | *(no implementado)* | Factura generada tras pago aprobado |
| **invoices** | `invoice-failed` | *(no implementado)* | Error al generar factura |
| **notifications** | `notification-sent` | *(no implementado)* | Notificación enviada al cliente |
| **notifications** | `notification-failed` | *(no implementado)* | Error al enviar notificación |
| **audit** | `audit-record-created` | *(no implementado)* | Registro de auditoría creado |

Todos los topics implementados tienen **3 particiones** y **factor de replicación 1** (configuración de laboratorio).

## 4. Claves de particionamiento

| Topic | Clave | Justificación |
|---|---|---|
| `orders` | `orderId` | Todos los eventos de un mismo pedido caen en la misma partición, preservando el orden de procesamiento por pedido |
| `payments` | `orderId` | Misma clave que el evento original permite correlacionar pago con su pedido en la misma partición |
| `inventory` | `orderId` | Idem, permite correlacionar inventario con su pedido |
| `invoices` | `orderId` | Idem, factura asociada al pedido |
| `notifications` | `orderId` | Idem, notificaciones asociadas al pedido |
| **audit** | `correlationId` | Se usa un `correlationId` (UUID de la transacción completa) en lugar de `orderId` porque el topic `audit` agrupa eventos de múltiples dominios; el `correlationId` permite reconstruir la traza completa de una operación en una sola partición |

Usar `orderId` como clave de particionamiento garantiza que los eventos de cada pedido se entreguen en orden secuencial a los consumidores, lo cual es crítico para la corrección del flujo de negocio (ej: no processar inventario antes de haber creado el pedido).

## 5. Consumer Groups

Cada servicio tiene su propio Consumer Group, lo que permite que todos reciban una copia completa e independiente de cada mensaje:

| Consumer Group | Servicio |
|---|---|
| `order-service` | order-service (grupo por defecto, aunque es productor) |
| `payment-service` | payment-service |
| `inventory-service` | inventory-service |
| `invoice-service` | invoice-service (propuesto) |
| `notification-service` | notification-service |
| `analytics-service` | analytics-service |
| `audit-service` | audit-service (propuesto) |

**¿Por qué grupos distintos?**

Si dos servicios compartieran el mismo Consumer Group, Kafka balancearía las particiones entre ellos: cada mensaje se entregaría a solo uno de los dos. Esto es útil para escalar horizontalmente un mismo servicio, pero no cuando servicios distintos necesitan procesar el mismo evento de forma independiente. Con grupos separados, cada servicio recibe todos los mensajes del topic, permitiendo que pago, inventario, notificaciones y analítica procesen el mismo evento de `orders` sin interferencia.

## 6. Por qué no usar un único topic `events` global

| Problema | Consecuencia |
|---|---|
| **Pérdida de cohesión por dominio** | Mezclar eventos de órdenes, pagos, inventario y facturas en un solo topic dificulta entender el propósito del topic |
| **Acoplamiento de esquemas** | Todos los eventos compartirían el mismo schema o un schema genérico, perdiendo typed-safety y obligando a los consumidores a filtrar y parsear manualmente |
| **Dificultad de escalar particiones** | Cada flujo tiene volúmenes distintos (ej: auditoría genera muchos más eventos que pagos). Un solo topic fuerza a dimensionar particiones para el peor caso |
| **Mezcla de retenciones y SLAs** | Los eventos de auditoría pueden requerir retención de meses; los de notificaciones, horas. Con un solo topic, la política de retención es única |
| **Complejidad de filtrado** | Los consumidores deben recibir todos los eventos y filtrar por tipo, aumentando la carga innecesaria y la complejidad del código |

En su lugar, se usan topics específicos por dominio (`orders`, `payments`, `inventory`, `invoices`, `notifications`, `audit`), cada uno con su propio esquema, número de particiones, políticas de retención y SLAs.

## 7. Estrategia de errores

### Tipos de error

| Tipo | Ejemplo | Acción |
|---|---|---|
| **Transitorio** | Red caída, broker temporalmente no disponible | Reintento automático con backoff |
| **Permanente** | Mensaje corrupto, schema inválido | Dead Letter Topic (DLT) sin reintento |
| **Negocio** | `customerId` vacío, total inválido | Excepción → reintentos → DLT |
| **Técnico** | NullPointerException, error de configuración | Reintentos → DLT + log de error |

### Política de reintentos y DLT

- **Backoff**: `FixedBackOff(2000L, 3L)` — 3 reintentos con 2 segundos de intervalo entre cada uno
- **DLT**: Si fallan los 3 reintentos, el mensaje se publica automáticamente en el topic `<original>.DLT` con la misma clave de particionamiento
- **Recuperación**: Los mensajes en DLT pueden revisarse manualmente y re-publicarse al topic original tras corregir la causa

### Idempotencia

Cada evento tiene un `eventId` único (UUID). Los consumidores mantienen un `Set` en memoria de `eventId`s procesados (`ConcurrentHashMap.newKeySet()`). Antes de procesar un evento, verifican si ya fue procesado; si es así, lo saltan.

**Mejora para producción**: En lugar de un `Set` en memoria (que se pierde al reiniciar), usar una tabla en base de datos con `eventId` como clave única y estado de procesamiento.

## 8. Consistencia eventual

### Estados del pedido

```
                     ┌─ PAYMENT_APPROVED ─┐
                     │                    │
CREATED ─────┬───────┤                    ├── CONFIRMED
             │       │                    │
             │       └─ INVENTORY_RESERVED┘
             │
             ├─ PAYMENT_REJECTED ──────── CANCELLED
             │
             └─ INVENTORY_REJECTED ────── CANCELLED
```

El pedido se confirma solo si tanto el pago como el inventario son exitosos. Si alguno falla, el pedido se cancela.

### Aceptación de consistencia eventual

- No existe una transacción distribuida entre pago e inventario
- Cada servicio procesa su evento de forma independiente y asíncrona
- El estado final del pedido es la composición de los resultados de ambos servicios
- Puede haber una ventana de tiempo donde el pago esté aprobado pero el inventario aún no se haya procesado (o viceversa)
- En producción, un servicio de compensación o un orchestrator (Saga pattern) podría manejar estos casos

## 9. Síncrono vs. asíncrono

### Procesos síncronos (REST)

- **Creación de pedido**: `POST /orders` — el cliente necesita una respuesta inmediata con el ID del pedido
- **Consulta de catálogo**: el cliente debe ver productos y precios en tiempo real
- **Autenticación**: `login` / `registro` requieren validación inmediata
- **Consulta de estado de pedido**: el usuario consulta si su pedido fue aprobado

### Procesos asíncronos (Kafka)

- **Procesamiento de pagos**: se hace en segundo plano tras crear el pedido
- **Verificación de inventario**: se hace en segundo plano
- **Notificaciones**: correos/SMS se envían sin bloquear al usuario
- **Analítica**: métricas y contadores se actualizan de forma asíncrona
- **Auditoría**: el registro de eventos se hace sin impacto en el flujo principal
- **Generación de facturas**: se produce después de confirmar el pago

### ¿Por qué no todo por REST?

Los procesos asíncronos (pago, inventario, notificaciones) no requieren respuesta inmediata del usuario y pueden demorar segundos o minutos. Usar Kafka para estos flujos permite:
- Desacoplar los servicios (cada uno escala independientemente)
- Tolerancia a fallos (si un servicio cae, los mensajes se acumulan en Kafka y se procesan cuando vuelve)
- Mayor throughput (los productores no se bloquean esperando respuestas)

## 10. Riesgos y mejoras para producción

| Riesgo | Mejora propuesta |
|---|---|
| **Factor de replicación 1** (pérdida de datos si el broker falla) | Aumentar a 3 en producción para tolerancia a fallos |
| **Sin monitoreo de lag** (no se detectan consumidores atrasados) | Integrar Prometheus + Grafana con métricas de Kafka (lag por consumer group) |
| **Particiones fijas** (no escalan con el volumen) | Dimensionar particiones según volumen proyectado por topic; monitorear y reparticionar si es necesario |
| **Retención por defecto** (7 días, puede ser insuficiente para auditoría) | Configurar retención por topic: 30-90 días para `audit`, 7 días para `notifications`, etc. |
| **Eventos sin versionado** (cambios de schema rompen consumidores) | Usar Schema Registry (Avro o Protobuf) con control de compatibilidad hacia atrás |
| **Idempotencia en memoria** (se pierde al reiniciar) | Almacenar `eventId` procesados en base de datos (tabla `processed_events`) |
| **Sin autenticación en Kafka** (cualquiera puede publicar/consumir) | Habilitar SASL/SSL y ACLs en producción |
| **Sin órquestación de sagas** (inconsistencia si pago se aprueba pero inventario se rechaza) | Implementar Saga pattern (coreografía u orquestación) con eventos de compensación |

---

## Respuestas a las actividades teóricas

### Actividad 1 — Clasificación síncrono/asíncrono/híbrido (Capítulo 1)

Clasificación de procesos de una tienda en línea:

| Proceso | Tipo | Justificación |
|---|---|---|
| Consultar catálogo de productos | Síncrono | El usuario necesita ver los productos inmediatamente al cargar la página |
| Autenticación (login) | Síncrono | Debe validarse en el momento, respuesta inmediata |
| Crear pedido | Híbrido | El endpoint REST es síncrono (devuelve el ID del pedido), pero el procesamiento de pago/inventario es asíncrono |
| Procesar pago | Asíncrono | Se ejecuta en segundo plano tras la creación del pedido |
| Verificar inventario | Asíncrono | Se ejecuta en segundo plano |
| Enviar notificación (email/SMS) | Asíncrono | No bloquea al usuario, puede demorar segundos |
| Generar factura | Asíncrono | Se produce después del pago, sin intervención del usuario |
| Registrar auditoría | Asíncrono | Debe ocurrir siempre, pero sin impacto en la experiencia del usuario |
| Consultar estado del pedido | Síncrono | El usuario espera la respuesta inmediata al consultar |

### Actividad 2 — Riesgos de un topic mal configurado (Capítulo 2)

**Configuración analizada:** 1 partición, replicación 1, sin clave, retención de 24h.

| Problema | Riesgo | Atributo de calidad afectado |
|---|---|---|
| **1 partición** | Cuello de botella: solo un consumidor por grupo puede leer, el throughput está limitado por esa única partición. No hay paralelismo de consumo | Rendimiento, Escalabilidad |
| **Replicación 1** | Si el broker falla, se pierden todos los mensajes del topic. No hay tolerancia a fallos | Disponibilidad, Durabilidad |
| **Sin clave** | Los mensajes se distribuyen en round-robin entre particiones (en este caso solo hay 1, pero el problema se agrava si se añaden más). No hay orden garantizado por entidad de negocio (ej: pedido). Un consumidor no puede asumir orden por `orderId` | Consistencia, Orden |
| **Retención de 24h** | Si un consumidor falla por más de 24 horas, pierde mensajes irreversiblemente. Imposible reprocesar datos históricos | Confiabilidad, Recuperabilidad |

**Propuesta de mejora:**
- Aumentar a 3-6 particiones (según volumen esperado)
- Replication factor 3 para tolerancia a fallos
- Usar clave de particionamiento (`orderId` para topics de negocio) para preservar orden
- Retención de 7-30 días según necesidades de reprocesamiento y auditoría

### Actividad 5 — Diseño completo del flujo de compra (Capítulo 5)

**Eventos, topics, productores y consumidores:**

| Topic | Evento | Productor | Consumidores (Consumer Group) |
|---|---|---|---|
| `orders` | `order-created` | `OrderEventProducer` | `payment-service`, `inventory-service`, `analytics-service` |
| `payments` | `payment-approved`, `payment-rejected` | `PaymentEventProducer` | `notification-service`, `analytics-service`, `invoice-service` |
| `inventory` | `inventory-reserved`, `inventory-rejected` | `InventoryEventProducer` | `notification-service`, `analytics-service` |
| `invoices` | `invoice-generated`, `invoice-failed` | `InvoiceEventProducer` | `notification-service`, `analytics-service`, `audit-service` |
| `notifications` | `notification-sent`, `notification-failed` | `NotificationEventProducer` | `audit-service` |
| `audit` | `audit-record-created` | `AuditEventProducer` | *(almacenamiento)* |

**Claves de particionamiento:** `orderId` para todos los topics de negocio; `correlationId` para `audit`.

**Consumer Groups:** Uno por servicio, para que cada uno reciba copia independiente de cada mensaje.

**Justificación de por qué no usar un topic `events` único:** Ver sección 6 de este documento.

### Actividad 7 — Estrategia de errores para inventory-service (Capítulo 7)

**Cuándo reintentar:**
- Errores transitorios: conexión a base de datos de inventario timeout, broker de Kafka temporalmente no disponible (`NotEnoughReplicasException`, `NetworkException`)
- Validaciones de negocio recuperables: datos incompletos que puedan resolverse con reintento (ej: esperar a que otro servicio complete un prerequisito)

**Cuándo enviar a DLT (no reintentar):**
- Errores permanentes: mensaje corrupto, schema inválido (`SerializationException`)
- Violaciones de reglas de negocio no recuperables: producto inexistente en catálogo, inventario agotado definitivamente
- Errores de configuración: conexión a BD con credenciales inválidas

**Qué revisar en el DLT:**
- El contenido del mensaje original (header, key, value)
- La excepción que causó el fallo (stacktrace)
- El timestamp del error
- El topic original y partición

**Cómo evitar bucles infinitos:**
- No configurar un error handler en el propio consumidor del DLT (o si se configura, no enviar de vuelta al topic original sin modificación)
- Usar un número máximo de reintentos (3 en nuestra implementación)
- Incluir un header de "número de reintentos" y no reintentar si ya se superó un máximo global
- Monitorear el DLT con alertas para intervención manual

### Actividad 8 — Diagnóstico arquitectura problemática (Capítulo 8)

**Configuración analizada:** topic `events` único, sin clave, replicación 1, sin DLT, sin monitoreo de lag.

| Problema | Atributo de calidad afectado | Prioridad |
|---|---|---|
| **Topic único** mezcla eventos de todos los dominios | Mantenibilidad, Cohesión | Alta |
| **Sin clave de particionamiento** → sin orden por entidad de negocio | Consistencia | Alta |
| **Replicación 1** → pérdida total si el broker falla | Disponibilidad, Durabilidad | Crítica |
| **Sin DLT** → mensajes erróneos se pierden para siempre | Confiabilidad, Recuperabilidad | Alta |
| **Sin monitoreo de lag** → no se detectan consumidores caídos o atrasados | Observabilidad | Media |

**Mejoras prioritarias (ordenadas por impacto):**

1. **Crítica:** Replication factor ≥ 3 — sin esto, un solo fallo de broker causa pérdida de datos
2. **Alta:** Topics específicos por dominio — `orders`, `payments`, `inventory`, etc., cada uno con su schema y particiones
3. **Alta:** Clave de particionamiento (`orderId`, `correlationId`) para preservar orden y correlación
4. **Alta:** Dead Letter Topics para mensajes que fallan después de reintentos
5. **Media:** Monitoreo de lag con Prometheus/Grafana o Kafka UI para detectar consumidores atrasados
6. **Media:** Configurar retención adecuada por tipo de evento (ej: 7 días para notificaciones, 90 días para auditoría)

### Actividades del Capítulo 9 — Consolidación

#### a) Clasificación REST / Kafka / Híbrido de 8 procesos

| # | Proceso | Tipo | Justificación |
|---|---|---|---|
| 1 | Consultar catálogo | REST | Respuesta inmediata requerida |
| 2 | Iniciar sesión | REST | Autenticación síncrona |
| 3 | Crear pedido | Híbrido | REST para recibir el pedido, Kafka para procesarlo |
| 4 | Pagar pedido | Kafka | Procesamiento asíncrono en segundo plano |
| 5 | Verificar inventario | Kafka | Procesamiento asíncrono |
| 6 | Enviar notificación | Kafka | No bloquea al usuario |
| 7 | Generar factura | Kafka | Depende de pago, asíncrono |
| 8 | Consultar estado del pedido | REST | El usuario espera respuesta inmediata |

#### b) Diagnóstico de configuración propuesta en 9.3

**Configuración:** 1 partición, replicación 1, retención 12h, sin clave, sin `eventId`/`correlationId`, un solo Consumer Group para todos los servicios, sin DLT, sin monitoreo de lag.

**Problemas identificados:**

| Problema | Consecuencia |
|---|---|
| 1 partición | Sin paralelismo de consumo; throughput máximo = capacidad de un solo consumidor |
| Replicación 1 | Pérdida total de datos si el broker falla |
| Retención 12h | Consumidores con más de 12h de retraso pierden mensajes irrevocablemente |
| Sin clave | Sin orden garantizado por entidad de negocio |
| Sin `eventId`/`correlationId` | Imposible detectar duplicados; sin trazabilidad entre eventos del mismo flujo |
| Un solo Consumer Group | Todos los servicios compiten por las mismas particiones; solo uno recibe cada mensaje. `payment-service`, `inventory-service`, `notification-service` no podrían todos recibir el mismo evento de `orders` |
| Sin DLT | Mensajes erróneos se descartan y no hay posibilidad de depuración o reprocesamiento |
| Sin monitoreo de lag | No se detectan consumidores caídos o degradados hasta que el usuario reporta el problema |

**Mejoras prioritarias:**
1. Crear Consumer Groups separados por servicio (crítico — sin esto la arquitectura no funciona)
2. Aumentar particiones a 3+ y replication factor a 3
3. Agregar clave de particionamiento y `eventId`/`correlationId` en los eventos
4. Configurar DLT para manejo de errores
5. Extender retención a mínimo 7 días
6. Monitoreo de lag con Kafka UI o herramientas dedicadas

---

*Documento generado como parte del laboratorio "Apache Kafka y Arquitecturas Orientadas por Eventos" — ARSW, Escuela Colombiana de Ingeniería Julio Garavito.*
