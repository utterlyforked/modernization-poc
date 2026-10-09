#!/bin/bash

echo "🔍 Legacy Modernization - Debug Script"
echo "========================================"
echo ""

echo "1️⃣ Checking container status..."
docker compose ps
echo ""

echo "2️⃣ Checking Debezium connectors..."
CONNECTORS=$(curl -s http://localhost:8083/connectors 2>/dev/null)
if [ -z "$CONNECTORS" ]; then
    echo "❌ Debezium API not responding"
else
    echo "✅ Debezium connectors: $CONNECTORS"

    # Check each connector status
    for connector in legacy-a1-connector legacy-a2-connector legacy-b1-connector legacy-b2-connector; do
        echo ""
        echo "Checking $connector..."
        curl -s http://localhost:8083/connectors/$connector/status 2>/dev/null | jq -r '.connector.state, .tasks[0].state' 2>/dev/null || echo "Not found"
    done
fi
echo ""

echo "3️⃣ Checking Kafka topics..."
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep legacy || echo "❌ No legacy topics found"
echo ""

echo "4️⃣ Checking legacy database data..."
echo "Legacy A1:"
docker exec -it postgres-legacy-a1 psql -U legacyuser -d legacy_a1 -c "SELECT COUNT(*) FROM person;" 2>/dev/null
echo ""

echo "5️⃣ Checking new system database..."
echo "Staging table:"
docker exec -it postgres-new-system psql -U newuser -d new_system -c "SELECT COUNT(*) FROM person_staging;" 2>/dev/null
echo ""
echo "Final person table:"
docker exec -it postgres-new-system psql -U newuser -d new_system -c "SELECT COUNT(*) FROM person;" 2>/dev/null
echo ""

echo "6️⃣ Recent consumer logs (last 30 lines)..."
docker compose logs --tail=30 cdc-consumer
echo ""

echo "7️⃣ Recent debezium logs (last 20 lines)..."
docker compose logs --tail=20 debezium
echo ""

echo "8️⃣ Testing Kafka message consumption..."
echo "Listening to legacy_a1 topic for 5 seconds..."
timeout 5 docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic legacy_a1.public.person \
    --from-beginning \
    --max-messages 1 2>/dev/null || echo "No messages found in 5 seconds"
echo ""

echo "✅ Debug complete. Check output above for issues."
