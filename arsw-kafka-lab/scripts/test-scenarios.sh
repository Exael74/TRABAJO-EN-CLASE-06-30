#!/bin/bash
# ======================================================
# Escenarios de prueba para el laboratorio Kafka ARSW
# ======================================================

BASE_URL="http://localhost:8081"
KAFKA_CONTAINER="arsw-kafka"
KAFKA_CMD="/opt/kafka/bin"

echo "========================================"
echo "  LABORATORIO KAFKA ARSW — PRUEBAS"
echo "========================================"

# --- 1. Crear pedidos con distintos totales ---
echo ""
echo "▶ Caso 1: Total \$50,000 → Pago APPROVED, Inventario RESERVED"
curl -s -X POST "$BASE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS01","total":50000}' | python3 -m json.tool 2>/dev/null || echo ""

echo ""
echo "▶ Caso 2: Total \$200,000 → Pago APPROVED, Inventario RESERVED"
curl -s -X POST "$BASE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS02","total":200000}' | python3 -m json.tool 2>/dev/null || echo ""

echo ""
echo "▶ Caso 3: Total \$280,000 → Pago REJECTED, Inventario RESERVED"
curl -s -X POST "$BASE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS03","total":280000}' | python3 -m json.tool 2>/dev/null || echo ""

echo ""
echo "▶ Caso 4: Total \$400,000 → Pago REJECTED, Inventario REJECTED"
curl -s -X POST "$BASE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS04","total":400000}' | python3 -m json.tool 2>/dev/null || echo ""

echo ""

# --- 2. Ver datos en Kafka (timestamps y offsets) ---
echo "========================================"
echo "  VERIFICAR EN KAFKA"
echo "========================================"
echo ""
echo "▶ Topics creados:"
docker exec -it "$KAFKA_CONTAINER" $KAFKA_CMD/kafka-topics.sh --bootstrap-server localhost:9092 --list

echo ""
echo "▶ Mensajes en topic 'orders' (últimos 10):"
docker exec -it "$KAFKA_CONTAINER" $KAFKA_CMD/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 10 \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --timeout-ms 3000 2>/dev/null

echo ""
echo "▶ Mensajes en topic 'payments' (últimos 10):"
docker exec -it "$KAFKA_CONTAINER" $KAFKA_CMD/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payments --from-beginning --max-messages 10 \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --timeout-ms 3000 2>/dev/null

echo ""
echo "▶ Mensajes en topic 'inventory' (últimos 10):"
docker exec -it "$KAFKA_CONTAINER" $KAFKA_CMD/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic inventory --from-beginning --max-messages 10 \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --timeout-ms 3000 2>/dev/null

# --- 3. Ver lag por Consumer Group ---
echo ""
echo "========================================"
echo "  LAG POR CONSUMER GROUP"
echo "========================================"
echo ""

for group in "order-service" "payment-service" "inventory-service" "notification-service" "analytics-service"; do
  echo "▶ Consumer Group: $group"
  docker exec -it "$KAFKA_CONTAINER" $KAFKA_CMD/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --group "$group" --describe 2>/dev/null
  echo ""
done

# --- 4. Probar DLT (customerId vacío) ---
echo ""
echo "========================================"
echo "  PRUEBA DLT — customerId vacío"
echo "========================================"
echo ""
echo "▶ Enviando pedido con customerId vacío (debería ir a orders.DLT tras 3 reintentos):"
curl -s -X POST "$BASE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"customerId":"","total":50000}' | python3 -m json.tool 2>/dev/null || echo ""

echo ""
echo "▶ Esperando 10s para que los reintentos y DLT se procesen..."
sleep 10

echo ""
echo "▶ Mensajes en topic 'orders.DLT':"
docker exec -it "$KAFKA_CONTAINER" $KAFKA_CMD/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.DLT --from-beginning --max-messages 5 \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --timeout-ms 3000 2>/dev/null

echo ""
echo "========================================"
echo "  PRUEBAS COMPLETADAS"
echo "========================================"
echo ""
echo "Resumen de resultados esperados:"
echo "┌──────────┬──────────┬────────────────┬──────────────────┐"
echo "│ Total    │ customer │ Pago           │ Inventario       │"
echo "├──────────┼──────────┼────────────────┼──────────────────┤"
echo "│ \$50,000  │ CUS01    │ APPROVED       │ RESERVED         │"
echo "│ \$200,000 │ CUS02    │ APPROVED       │ RESERVED         │"
echo "│ \$280,000 │ CUS03    │ REJECTED       │ RESERVED         │"
echo "│ \$400,000 │ CUS04    │ REJECTED       │ REJECTED         │"
echo "│ \$50,000  │ (vacio)  │ orders.DLT     │ —               │"
echo "└──────────┴──────────┴────────────────┴──────────────────┘"
