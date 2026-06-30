#!/bin/bash
# Crear topics obligatorios del laboratorio

TOPICS=("orders" "payments" "inventory")
PARTITIONS=3
REPLICATION=1
BOOTSTRAP="localhost:9092"

echo "Creando topics..."
for topic in "${TOPICS[@]}"; do
  docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$REPLICATION"
done

echo ""
echo "Topics creados:"
docker exec -it arsw-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
