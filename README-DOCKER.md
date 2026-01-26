# Legacy Modernization PoC - Docker Multi-Container Setup

This is an enhanced version of the modernization demo using:
- **PostgreSQL** instead of SQLite (real database engine)
- **Debezium** for CDC (log-based, not trigger-based)
- **Kafka** for event streaming
- **Schema Registry** for Avro schemas
- **dbt** for data transformations
- **Docker Compose** for multi-container orchestration

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Legacy Systems (4 PostgreSQL instances)                        │
│  - postgres-legacy-a1, a2 (Schema: data_1, data_2, data_3)     │
│  - postgres-legacy-b1, b2 (Schema: data_a, data_b, data_c)     │
└──────────────────┬──────────────────────────────────────────────┘
                   │ WAL / Logical Replication
                   ↓
┌─────────────────────────────────────────────────────────────────┐
│  Debezium Connect (CDC Connectors)                              │
│  - Reads PostgreSQL Write-Ahead Log                             │
│  - Converts changes to Kafka events                             │
└──────────────────┬──────────────────────────────────────────────┘
                   │ Avro-serialized events
                   ↓
┌─────────────────────────────────────────────────────────────────┐
│  Kafka + Schema Registry                                        │
│  - Topics: legacy_a1.public.person, legacy_a2.public.person     │
│            legacy_b1.public.person, legacy_b2.public.person     │
└──────────────────┬──────────────────────────────────────────────┘
                   │ Consumer reads events
                   ↓
┌─────────────────────────────────────────────────────────────────┐
│  CDC Consumer (Python + dbt)                                    │
│  - Consumes from Kafka topics                                   │
│  - Stages to person_staging table                               │
│  - Runs dbt transformations every 5 seconds                     │
└──────────────────┬──────────────────────────────────────────────┘
                   │ Transforms staging → final
                   ↓
┌─────────────────────────────────────────────────────────────────┐
│  New System (postgres-new-system)                               │
│  - person_staging: Raw CDC events                               │
│  - person: Unified multi-tenant schema (dbt transformed)        │
└─────────────────────────────────────────────────────────────────┘
                   ↑
                   │ Write-through
┌─────────────────────────────────────────────────────────────────┐
│  API Service (Flask)                                            │
│  - GET /api/persons - Read from new system                      │
│  - POST /api/persons - Write-through to legacy                  │
│  - PUT /api/persons - Update via write-through                  │
└─────────────────────────────────────────────────────────────────┘
```

## What This Proves

### Real-World Components
✅ **PostgreSQL** - Production database with WAL-based replication
✅ **Debezium** - Industry-standard CDC tool (not custom triggers)
✅ **Kafka** - Event streaming platform with offset management
✅ **Schema Registry** - Avro schema management and evolution
✅ **dbt** - Data transformation layer (ELT pattern)
✅ **Container networking** - Services communicate over Docker network

### Still Missing (vs Real Production)
❌ Network latency - All containers on same host
❌ Network partitions - No network failure simulation
❌ Multi-datacenter - Geographic distribution
❌ Full HA setup - Single instances, not clusters
❌ Production monitoring - No Prometheus, Grafana, etc.

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available for Docker
- Ports 5000 (API) and 8000 (Web) available

## Quick Start

### 1. Start All Services

```bash
docker-compose up -d
```

This will start:
- 1 Zookeeper container
- 1 Kafka broker
- 1 Schema Registry
- 4 PostgreSQL legacy databases
- 1 PostgreSQL new system database
- 1 Debezium Connect
- 1 CDC Consumer (with dbt)
- 1 API service
- 1 Nginx web server
- 1 Debezium connector registration container (runs once)

### 2. Wait for Services to Start

```bash
# Watch logs to see when everything is ready (Ctrl+C to exit)
docker-compose logs -f

# Or check individual services
docker-compose ps
```

Wait until you see:
- `cdc-consumer` showing "Subscribed to topics"
- `debezium` showing connector registrations
- `api` showing "Starting API server"

This takes about 30-60 seconds on first startup.

### 3. Open the UI

Open browser to: http://localhost:8000

You should see:
- 4 legacy system panels (top) - old-school gray styling
- 1 modern system panel (bottom) - glassmorphic dark theme

## Usage

### Test CDC Flow (Legacy → New)

1. In any legacy panel, fill in the form:
   - First Name: "John"
   - Surname: "Doe"
   - Date of Birth: "1990-01-15"
   - City: "New York"
   - Data 2 (Legacy A) or Data C (Legacy B): "test_value"
2. Click "Write to Legacy DB"
3. Record appears immediately in legacy table
4. Wait ~5 seconds
5. Record appears in "Multi-tenant SAAS" panel with:
   - Correct tenant badge
   - Extra Field populated from Data 2 or Data C

### Test Write-Through (New → Legacy → New)

1. In "Multi-tenant SAAS" panel:
   - Select a tenant
   - Fill in form fields
   - Fill "Extra Field" (routes to data_2 or data_c)
2. Click "Create new record"
3. Record appears in corresponding legacy panel immediately
4. Wait ~5 seconds
5. Record syncs back to new system panel

### Test Updates

1. Click "Edit" button on any record
2. Modify fields
3. Click "Save Changes"
4. Update writes through to legacy
5. Syncs back via CDC

## Architecture Components

### Services

**zookeeper** - Coordination service for Kafka
- Port: 2181
- Required for Kafka cluster management

**kafka** - Event streaming platform
- Port: 9092 (internal), 29092 (broker)
- Stores CDC events from Debezium

**schema-registry** - Avro schema management
- Port: 8081
- Manages schema versions for Kafka messages

**postgres-legacy-{a1,a2,b1,b2}** - Legacy databases
- Each runs PostgreSQL 15 with `wal_level=logical`
- Different schemas (Legacy A vs B)
- Sample data pre-loaded

**postgres-new-system** - Modern database
- Multi-tenant schema
- person_staging (raw CDC events)
- person (transformed unified data)

**debezium** - CDC connector platform
- Port: 8083
- Reads PostgreSQL WAL logs
- Publishes to Kafka topics
- 4 connectors (one per legacy instance)

**cdc-consumer** - Event consumer + transformer
- Consumes from Kafka topics
- Stages to person_staging
- Runs dbt every 5 seconds
- Marks processed records

**api** - Flask REST API
- Port: 5000
- Handles reads and write-through
- Connects to all databases

**web** - Nginx static server
- Port: 8000
- Serves index.html

### Data Flow

**Sync (Legacy → New):**
1. INSERT/UPDATE in legacy PostgreSQL
2. PostgreSQL WAL log updated
3. Debezium reads WAL via logical replication
4. Event published to Kafka topic with Avro schema
5. Consumer reads from Kafka
6. Event staged to person_staging
7. dbt transforms staging → person table
8. Appears in UI

**Write-Through (New → Legacy):**
1. POST to /api/persons
2. API writes to correct legacy PostgreSQL
3. PostgreSQL WAL updated
4. Debezium captures change
5. Flows through normal sync path

## Monitoring and Debugging

### View Service Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f cdc-consumer
docker-compose logs -f debezium
docker-compose logs -f api
```

### Check Kafka Topics

```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume from topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic legacy_a1.public.person \
  --from-beginning
```

### Check Debezium Connectors

```bash
# List connectors
curl http://localhost:8083/connectors

# Check connector status
curl http://localhost:8083/connectors/legacy-a1-connector/status
```

### Inspect Databases

```bash
# Connect to legacy database
docker exec -it postgres-legacy-a1 psql -U legacyuser -d legacy_a1

# Check persons
SELECT * FROM person;

# Connect to new system
docker exec -it postgres-new-system psql -U newuser -d new_system

# Check staging
SELECT * FROM person_staging;

# Check transformed data
SELECT * FROM person;
```

### Run dbt Manually

```bash
# Enter consumer container
docker exec -it cdc-consumer bash

# Run dbt
dbt run --project-dir /dbt --profiles-dir /dbt

# Check models
dbt ls --project-dir /dbt --profiles-dir /dbt
```

## Common Issues

### Services Won't Start

```bash
# Check if ports are already in use
lsof -i :5000
lsof -i :8000

# Reset everything
docker-compose down -v
docker-compose up -d
```

### No Data Syncing

1. Check Debezium connectors are registered:
   ```bash
   curl http://localhost:8083/connectors
   ```

2. Check Kafka topics exist:
   ```bash
   docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

3. Check consumer is running:
   ```bash
   docker-compose logs cdc-consumer | tail -50
   ```

4. Check staging table has data:
   ```bash
   docker exec -it postgres-new-system psql -U newuser -d new_system -c "SELECT COUNT(*) FROM person_staging;"
   ```

### Debezium Not Capturing Changes

PostgreSQL must have `wal_level=logical`. This is set in docker-compose.yml:
```yaml
command:
  - "postgres"
  - "-c"
  - "wal_level=logical"
```

Check it's set:
```bash
docker exec -it postgres-legacy-a1 psql -U legacyuser -d legacy_a1 -c "SHOW wal_level;"
```

## Cleanup

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (deletes all data)
docker-compose down -v

# Remove images
docker-compose down -v --rmi all
```

## Next Steps

To make this more realistic:

1. **Add network simulation** - Use Toxiproxy to inject latency/failures
2. **Scale components** - Multiple Kafka brokers, Debezium instances
3. **Add monitoring** - Prometheus + Grafana for metrics
4. **Add backfill** - Script to migrate existing data
5. **Add conflict resolution** - Handle concurrent updates
6. **Test failure scenarios** - Kill services, test recovery

## File Structure

```
.
├── docker-compose.yml           # Multi-container orchestration
├── postgres-init/               # DB initialization scripts
│   ├── legacy_a1.sql
│   ├── legacy_a2.sql
│   ├── legacy_b1.sql
│   ├── legacy_b2.sql
│   └── new_system.sql
├── debezium-connectors/         # Connector configurations
│   ├── legacy-a1.json
│   ├── legacy-a2.json
│   ├── legacy-b1.json
│   └── legacy-b2.json
├── scripts/
│   └── register-connectors.sh  # Auto-register connectors
├── dbt-project/                 # dbt transformation project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── schema.yml
│       └── person_unified.sql
├── cdc-consumer/                # Kafka consumer + dbt runner
│   ├── consumer.py
│   └── requirements.txt
├── api/                         # Flask API service
│   ├── api.py
│   └── requirements.txt
├── index.html                   # Web UI
├── Dockerfile.consumer          # CDC consumer image
├── Dockerfile.api               # API service image
└── README-DOCKER.md             # This file
```

## Comparison: SQLite vs Docker Setup

| Aspect | SQLite Version | Docker Version |
|--------|----------------|----------------|
| **Database** | SQLite files | PostgreSQL containers |
| **CDC** | Manual triggers | Debezium (WAL-based) |
| **Events** | Polling changelog | Kafka topics |
| **Transformation** | Python code | dbt models |
| **Networking** | Localhost | Docker network |
| **Schema Management** | None | Schema Registry (Avro) |
| **Deployment** | 3 Python scripts | 11 containers |
| **Realism** | Proof of concept | Closer to production |
| **Complexity** | Low | Medium |

## What You Learned

By running this setup, you now understand:

1. **Real CDC tooling** - Debezium configuration and operation
2. **Event streaming** - Kafka topics, offsets, consumer groups
3. **Schema management** - Avro schemas and evolution
4. **ELT pattern** - Staging → Transform with dbt
5. **Container orchestration** - Multi-service Docker Compose
6. **Service dependencies** - Healthchecks and startup ordering
7. **Production considerations** - What's still missing for production

This setup demonstrates the architecture pattern at a realistic level while remaining deployable on a single machine.
