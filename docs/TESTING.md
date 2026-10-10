# Testing Guide - Docker Multi-Container Setup

## Automated tests

Everything runs in Docker; the host only needs Docker, compose and make. Reports go to `test-results/`.

```bash
make test-unit        # unit tests, no stack needed
make test-dbt         # dbt model tests against a throwaway Postgres
make down             # the e2e suites need the dev stack stopped
make test-e2e         # e2e tests; brings up and tears down its own stack
make test-resilience  # stops containers mid-flight (own stack)
make test             # all four suites
```

CI (`.github/workflows/test.yml`) runs unit, dbt and e2e on every push and pull request, and uploads `test-results/`
(JUnit XML, summaries, stack logs) as an artifact.

The rest of this guide is a manual runbook for exploring the running stack (`make up`). The commands use
`docker exec -it`, which needs a terminal. In scripts or CI, drop the `-t` (use `docker exec -i`, or plain
`docker exec`), otherwise they fail with "the input device is not a TTY". `scripts/debug.sh` already does this.

---

# Manual Testing Guide

This guide helps you validate that all components are working correctly.

## Pre-Flight Checks

### 1. Verify All Containers Are Running

```bash
docker compose ps
```

Expected output: All services should show "Up" status:
- zookeeper
- kafka
- schema-registry
- postgres-legacy-a1, a2, b1, b2
- postgres-new-system
- debezium
- cdc-consumer
- api
- web

### 2. Check Debezium Connectors

```bash
curl http://localhost:8083/connectors
```

Should return:
```json
["legacy-a1-connector","legacy-a2-connector","legacy-b1-connector","legacy-b2-connector"]
```

Check connector status:
```bash
curl http://localhost:8083/connectors/legacy-a1-connector/status | jq
```

Should show `"state": "RUNNING"`

### 3. Check Kafka Topics

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Should show topics including:
- legacy_a1.public.person
- legacy_a2.public.person
- legacy_b1.public.person
- legacy_b2.public.person

## Test Scenarios

### Test 1: PostgreSQL to Kafka (Debezium)

**Goal:** Verify Debezium captures changes from PostgreSQL WAL

```bash
# Insert directly into legacy database
docker exec -it postgres-legacy-a1 psql -U legacyuser -d legacy_a1 -c \
  "INSERT INTO person (firstname, surname, date_of_birth, city, data_1, data_2, data_3) \
   VALUES ('Test', 'User', '1990-01-01', 'TestCity', 'val1', 'test_extra', 'val3');"

# Wait 2 seconds, then check Kafka topic
sleep 2

docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic legacy_a1.public.person \
  --from-beginning \
  --max-messages 1
```

**Expected:** You should see a JSON message with the inserted data

### Test 2: Kafka to Staging (Consumer)

**Goal:** Verify consumer reads from Kafka and stages data

```bash
# Check consumer logs
docker compose logs cdc-consumer | tail -20

# Check staging table
docker exec -it postgres-new-system psql -U newuser -d new_system -c \
  "SELECT tenant_id, source_id, firstname, surname, data_1, data_2, data_3, data_a, data_b, data_c FROM person_staging ORDER BY created_at DESC LIMIT 5;"
```

**Expected:** Records from Kafka should appear in person_staging. Staging keeps the raw source columns (`data_1..3` for Legacy A, `data_a..c` for Legacy B); `extra_field` only exists in `person`, after dbt.

### Test 3: dbt Transformation (Staging to Final)

**Goal:** Verify dbt transforms staging data to unified schema

```bash
# Check person table (dbt target)
docker exec -it postgres-new-system psql -U newuser -d new_system -c \
  "SELECT tenant_id, legacy_id, firstname, surname, extra_field, synced_at FROM person ORDER BY synced_at DESC LIMIT 5;"
```

**Expected:** Records should be transformed with:
- data_2 → extra_field (for Legacy A)
- data_c → extra_field (for Legacy B)
- Proper tenant_id assigned

### Test 4: Write-Through API

**Goal:** Verify write-through writes to legacy and syncs back

```bash
# Write via API
curl -X POST http://localhost:5000/api/persons \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "tenant_a1",
    "firstname": "API",
    "surname": "Test",
    "date_of_birth": "1995-06-15",
    "city": "API City",
    "extra_field": "api_value"
  }'

# Check it was written to legacy
docker exec -it postgres-legacy-a1 psql -U legacyuser -d legacy_a1 -c \
  "SELECT * FROM person WHERE firstname = 'API';"

# Wait for CDC sync (5 seconds)
sleep 5

# Check it synced back to new system
curl http://localhost:5000/api/persons | jq '.[] | select(.firstname == "API")'
```

**Expected:** Record appears in legacy immediately, then syncs back to new system

### Test 5: Schema Mapping

**Goal:** Verify different schemas map correctly

```bash
# Insert into Legacy A (data_2 field)
docker exec -it postgres-legacy-a1 psql -U legacyuser -d legacy_a1 -c \
  "INSERT INTO person (firstname, surname, date_of_birth, city, data_2) \
   VALUES ('MapA', 'Test', '1990-01-01', 'City', 'mapped_from_data_2');"

# Insert into Legacy B (data_c field)
docker exec -it postgres-legacy-b1 psql -U legacyuser -d legacy_b1 -c \
  "INSERT INTO person (firstname, surname, date_of_birth, city, data_c) \
   VALUES ('MapB', 'Test', '1990-01-01', 'City', 'mapped_from_data_c');"

# Wait for sync
sleep 5

# Check both mapped to extra_field
curl http://localhost:5000/api/persons | jq '.[] | select(.firstname | startswith("Map")) | {firstname, tenant_id, extra_field}'
```

**Expected:**
- MapA should have `extra_field: "mapped_from_data_2"` with `tenant_id: "tenant_a1"`
- MapB should have `extra_field: "mapped_from_data_c"` with `tenant_id: "tenant_b1"`

### Test 6: Multi-Tenant Isolation

**Goal:** Verify data is properly isolated by tenant

```bash
# Get count by tenant
curl http://localhost:5000/api/persons | jq 'group_by(.tenant_id) | map({tenant: .[0].tenant_id, count: length})'
```

**Expected:** JSON showing count per tenant

### Test 7: Update via Write-Through

**Goal:** Verify updates propagate correctly

```bash
# First, get a record
RECORD=$(curl -s http://localhost:5000/api/persons | jq '.[0]')
TENANT=$(echo $RECORD | jq -r '.tenant_id')
LEGACY_ID=$(echo $RECORD | jq -r '.legacy_id')

# Update via API
curl -X PUT http://localhost:5000/api/persons \
  -H "Content-Type: application/json" \
  -d "{
    \"tenant_id\": \"$TENANT\",
    \"legacy_id\": $LEGACY_ID,
    \"firstname\": \"Updated\",
    \"surname\": \"Name\",
    \"date_of_birth\": \"1990-01-01\",
    \"city\": \"New City\",
    \"extra_field\": \"updated_value\"
  }"

# Wait for sync
sleep 5

# Verify update
curl http://localhost:5000/api/persons | jq ".[] | select(.legacy_id == $LEGACY_ID)"
```

**Expected:** Record should show updated values after sync

## Performance Tests

### Test 8: Bulk Insert Load

**Goal:** Test how system handles multiple inserts

```bash
# Insert 100 records
for i in {1..100}; do
  docker exec -it postgres-legacy-a1 psql -U legacyuser -d legacy_a1 -c \
    "INSERT INTO person (firstname, surname, date_of_birth, city, data_2) \
     VALUES ('Bulk$i', 'Test', '1990-01-01', 'City', 'bulk_$i');" &
done
wait

# Wait for sync
sleep 10

# Check count
curl http://localhost:5000/api/persons | jq 'map(select(.firstname | startswith("Bulk"))) | length'
```

**Expected:** All 100 records should sync (may take 10-15 seconds)

### Test 9: Sync Lag Measurement

**Goal:** Measure time from insert to sync

```bash
# Insert with timestamp
START=$(date +%s)
docker exec -it postgres-legacy-a1 psql -U legacyuser -d legacy_a1 -c \
  "INSERT INTO person (firstname, surname, date_of_birth, city, data_2) \
   VALUES ('LagTest', 'User', '1990-01-01', 'City', 'lag_test');"

# Poll until appears in new system
while true; do
  RESULT=$(curl -s http://localhost:5000/api/persons | jq '.[] | select(.firstname == "LagTest")')
  if [ -n "$RESULT" ]; then
    END=$(date +%s)
    LAG=$((END - START))
    echo "Sync lag: $LAG seconds"
    break
  fi
  sleep 1
done
```

**Expected:** Lag should be 3-7 seconds (2s consumer poll + 5s dbt interval)

## Failure Scenarios

### Test 10: Consumer Restart

**Goal:** Verify consumer recovers after restart

```bash
# Stop consumer
docker compose stop cdc-consumer

# Insert data
docker exec -it postgres-legacy-a1 psql -U legacyuser -d legacy_a1 -c \
  "INSERT INTO person (firstname, surname, date_of_birth, city) \
   VALUES ('DowntimeTest', 'User', '1990-01-01', 'City');"

# Wait a bit
sleep 3

# Start consumer
docker compose start cdc-consumer

# Wait for catch-up
sleep 10

# Check data synced
curl http://localhost:5000/api/persons | jq '.[] | select(.firstname == "DowntimeTest")'
```

**Expected:** Data should sync after consumer restarts (Kafka preserves events)

### Test 11: Database Connection Loss

**Goal:** Verify system handles temporary database outage

```bash
# Stop legacy database
docker compose stop postgres-legacy-a1

# Try to write via API (should fail gracefully)
curl -X POST http://localhost:5000/api/persons \
  -H "Content-Type: application/json" \
  -d '{"tenant_id": "tenant_a1", "firstname": "Test", "surname": "User", "date_of_birth": "1990-01-01", "city": "City"}'

# Restart database
docker compose start postgres-legacy-a1

# Wait for recovery
sleep 10

# Verify system recovers
curl http://localhost:5000/api/health
```

**Expected:** API returns error during outage, recovers after restart

## Monitoring Commands

### View Live Consumer Activity

```bash
docker compose logs -f cdc-consumer
```

Look for:
- "Staged record: tenant=..."
- "Running dbt transformation..."
- "dbt transformation completed successfully"

### View Kafka Lag

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group cdc-consumer-group \
  --describe
```

Shows how far behind consumer is from latest messages

### Check Database Connections

```bash
# Legacy database connections
docker exec -it postgres-legacy-a1 psql -U legacyuser -d legacy_a1 -c \
  "SELECT count(*) FROM pg_stat_activity WHERE datname = 'legacy_a1';"

# New system connections
docker exec -it postgres-new-system psql -U newuser -d new_system -c \
  "SELECT count(*) FROM pg_stat_activity WHERE datname = 'new_system';"
```

## Cleanup Between Tests

```bash
# Reset all data
docker compose down -v
docker compose up -d

# Or just reset databases
docker compose restart postgres-legacy-a1 postgres-legacy-a2 postgres-legacy-b1 postgres-legacy-b2 postgres-new-system
```

## Success Criteria

✅ All containers running and healthy
✅ Debezium connectors in RUNNING state
✅ Kafka topics receiving events
✅ Consumer staging data within 2 seconds
✅ dbt transforming within 5 seconds
✅ Write-through completing immediately
✅ Round-trip sync completing within 7 seconds
✅ Schema mapping working correctly (data_2 and data_c → extra_field)
✅ Multi-tenant isolation working
✅ Updates propagating correctly
✅ System recovers from component restarts

## Troubleshooting

If tests fail, check:

1. **Container logs:** `docker compose logs <service>`
2. **Connector status:** `curl http://localhost:8083/connectors/<name>/status`
3. **Kafka topics:** Are messages arriving?
4. **Database contents:** Are records present?
5. **Network connectivity:** Can containers reach each other?

See README.md "Common Issues" section for detailed troubleshooting.
