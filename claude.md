# Legacy Modernization Demo - Claude Code Instructions

This is a working demonstration of the Unidirectional Sync + Write-Through pattern for legacy system modernization.

## Architecture Overview

**What this demonstrates:**
- 4 legacy database instances (2x Legacy A with schema data_1/data_2/data_3, 2x Legacy B with schema data_a/data_b/data_c)
- CDC triggers on each legacy instance capturing changes
- Sync adapter that maps different schemas to unified schema (data_2 and data_c both map to extra_field)
- Multi-tenant new system where all 4 instances sync to one database with tenant_id
- Write-through layer that routes new system writes to correct legacy DB
- Round-trip confirmation (write → legacy → CDC → new system)

## Project Structure

```
legacy_demo/
├── setup_legacy_a.py       # Creates 2 Legacy A instances (tenant_a1, tenant_a2)
├── setup_legacy_b.py       # Creates 2 Legacy B instances (tenant_b1, tenant_b2)
├── setup_new_system.py     # Creates multi-tenant database
├── sync_adapter.py         # CDC sync adapter (polls all 4 instances)
├── api.py                  # Flask API with write-through routing
├── index.html              # UI (legacy style top, modern style bottom)
├── start.sh                # Convenience script to run all services
├── Dockerfile              # Docker container setup
└── README.md               # Documentation
```

## Setup Instructions

### 1. Install Dependencies
```bash
pip install flask flask-cors
```

### 2. Setup Databases
Run these scripts to create all databases with sample data:
```bash
python3 setup_legacy_a.py
python3 setup_legacy_b.py
python3 setup_new_system.py
```

This creates:
- `legacy_a_tenant1.db` - Legacy A instance 1
- `legacy_a_tenant2.db` - Legacy A instance 2
- `legacy_b_tenant1.db` - Legacy B instance 1
- `legacy_b_tenant2.db` - Legacy B instance 2
- `new_system.db` - Multi-tenant new system

### 3. Start Services

You need 3 terminal windows:

**Terminal 1 - Sync Adapter:**
```bash
python3 sync_adapter.py
```
This polls all 4 legacy instances every 2 seconds, reads unprocessed changes, maps schemas, and syncs to new system.

**Terminal 2 - API Server:**
```bash
python3 api.py
```
Flask API running on port 5000. Handles:
- GET /api/persons - Read from new system (local reads)
- POST /api/persons - Write-through to legacy systems
- GET /api/legacy/{tenant_id}/persons - Direct legacy reads (for demo)

**Terminal 3 - Web Server:**
```bash
python3 -m http.server 8000
```
Serves the HTML UI on port 8000.

**Or use the convenience script:**
```bash
./start.sh
```
This starts all 3 services in the background.

### 4. Open UI
Open browser to: http://localhost:8000

## How to Test

### Test Sync Flow (Legacy → New)
1. Write a record in any of the 4 legacy panels (top of UI)
2. Fill in the form fields
3. Make sure to fill in the mapped field (Data 2 for Legacy A, Data C for Legacy B)
4. Click "Write to Legacy DB"
5. Watch the new system panel (bottom) - record appears in ~2 seconds with correct tenant badge

### Test Write-Through Flow (New → Legacy → New)
1. In the new system panel (bottom), fill in the form
2. Select a tenant (tenant_a1, tenant_a2, tenant_b1, or tenant_b2)
3. Fill in Extra Field (this routes to data_2 or data_c depending on tenant)
4. Click the write button
5. Check the corresponding legacy panel - record appears immediately
6. Check new system panel - record syncs back via CDC in ~2 seconds

### Test Field Mapping
1. Write "test123" in Data 2 field of Legacy A Instance 1
2. In new system, it appears as "test123" in Extra Field column with tenant_a1 badge
3. Write "test456" in Data C field of Legacy B Instance 2
4. In new system, it appears as "test456" in Extra Field column with tenant_b2 badge

## Troubleshooting

### No data showing
- Check all 3 services are running
- Look at terminal output for errors
- Check browser console (F12) for JavaScript errors
- Try manually refreshing: click the "R" button in each panel

### Sync not working
- Verify sync_adapter.py is running and not showing errors
- Check that databases were created successfully (should see .db files)
- Look for "Synced X records" messages in sync_adapter terminal

### API errors
- Test API directly: `curl http://localhost:5000/api/health`
- Should return JSON with status and list of tenants
- Check Flask isn't showing errors in terminal

### Write-through not working
- Check API terminal for errors when you submit
- Verify tenant_id is being sent in the POST request
- Check browser network tab (F12) to see the actual request/response

## Database Inspection

You can inspect the databases directly:

```bash
# Check legacy A instance 1
sqlite3 legacy_a_tenant1.db "SELECT * FROM person;"

# Check changelog to see CDC captured changes
sqlite3 legacy_a_tenant1.db "SELECT * FROM person_changelog;"

# Check new system
sqlite3 new_system.db "SELECT * FROM person;"

# Check tenant mappings
sqlite3 new_system.db "SELECT * FROM tenant_mapping;"
```

## Architecture Flows

### Sync Flow
```
Legacy DB 
  → CDC Trigger fires on INSERT/UPDATE
  → Writes to person_changelog table (processed=0)
  → Sync adapter polls changelog
  → Reads unprocessed records
  → Maps schema (data_2 → extra_field OR data_c → extra_field)
  → Writes to new_system.db with tenant_id
  → Marks changelog record as processed
```

### Write-Through Flow
```
New System UI
  → POST to /api/persons with tenant_id
  → API routes to correct legacy DB (tenant_a1 → legacy_a_tenant1.db)
  → Direct SQL INSERT to legacy database
  → Returns success to UI
  → CDC trigger captures the write
  → Sync adapter picks it up (round-trip confirmation)
  → Appears in new system with ~2 second delay
```

## Key Files Explained

**sync_adapter.py**
- Connects to all 4 legacy databases
- Polls every 2 seconds
- Maps legacy schemas to unified schema
- Handles tenant_id assignment
- Marks records as processed to avoid duplicates

**api.py**
- Flask API with CORS enabled
- TENANT_CONFIG maps tenant_id to database file and type
- WriteThrough.write_to_legacy() routes writes to correct legacy instance
- Different SQL for Legacy A (data_1/2/3) vs Legacy B (data_a/b/c)

**index.html**
- Top 4 panels: Legacy systems (old-school gray styling, Courier New font)
- Bottom panel: New system (modern glassmorphic design, dark theme)
- Auto-refreshes every 3 seconds
- Color-coded tenant badges

## Common Issues

**Port conflicts:**
If ports 5000 or 8000 are in use:
- Change Flask port in api.py: `app.run(host='0.0.0.0', port=5001)`
- Change HTTP server port: `python3 -m http.server 8001`
- Update HTML to point to new API port in the fetch URLs

**Database locked:**
If you get "database is locked" errors:
- Make sure only one sync_adapter.py is running
- Close any SQLite browser tools that might have the DB open
- Restart all services

**Changes not syncing:**
- Verify the sync adapter is actually running (not crashed)
- Check that CDC triggers were created (run setup scripts again)
- Look at person_changelog table to see if changes are being captured

## Next Steps / Extensions

If you want to extend this demo:
1. Add more legacy instances (just copy the setup pattern)
2. Add more fields to the schema (update both legacy and new schemas)
3. Add validation logic in the write-through layer
4. Add conflict resolution for concurrent updates
5. Add a UI for migration status tracking
6. Implement backfill logic for existing data
7. Add monitoring/metrics for sync lag

## Architecture Diagram

See the architecture diagram (SVG file) that shows:
- Legacy systems as source of truth
- Message queue for CDC events
- Sync/mapping layer with adapters
- Write-through layer for routing
- New multi-tenant system with unified schema

This demo proves the pattern works and can be scaled to hundreds of legacy instances!
