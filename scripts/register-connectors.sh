#!/bin/sh

# Wait for Debezium to be fully ready
echo "Waiting for Debezium Connect to be ready..."
sleep 20

# Register all connectors
for connector in /connectors/*.json; do
    echo "Registering connector: $connector"
    curl -X POST \
        -H "Content-Type: application/json" \
        --data @$connector \
        http://debezium:8083/connectors
    echo ""
    sleep 2
done

echo "All connectors registered!"

# List registered connectors
echo "Registered connectors:"
curl http://debezium:8083/connectors
echo ""
