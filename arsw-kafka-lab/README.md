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
docker exec -it arsw-kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 3 --replication-factor 1
docker exec -it arsw-kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic payments --partitions 3 --replication-factor 1
docker exec -it arsw-kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic inventory --partitions 3 --replication-factor 1
```

Verificar:

```bash
docker exec -it arsw-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
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

## Propuesta arquitectónica

(Contenido completo en `docs/propuesta-arquitectonica.md`)
