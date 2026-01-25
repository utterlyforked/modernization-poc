# Legacy Modernization Demo (Multi-Tenant)

A complete working demonstration of the Unidirectional Sync + Write-Through pattern with **multiple legacy instances** per system.

![Architecture](https://img.shields.io/badge/Architecture-Multi--Tenant-blue) ![Docker](https://img.shields.io/badge/Docker-Ready-success)

## 🚀 Quick Start (Docker - One Command!)

```bash
# Build and run everything
docker build -t legacy-demo .
docker run -p 5000:5000 -p 8000:8000 legacy-demo
```

Then open: **http://localhost:8000**

That's it! Everything is set up and running. 🎉

## 📦 What's Running

When you start the container, it automatically:
1. ✅ Creates 4 legacy database instances (2x Legacy A, 2x Legacy B)
2. ✅ Sets up the new multi-tenant database
3. ✅ Starts the CDC sync adapter (polls every 2 seconds)
4. ✅ Starts the Flask API server (port 5000)
5. ✅ Starts the web UI server (port 8000)

## 🏗️ Architecture

**4 Legacy Instances (Multi-Tenant Aware):**
- **Legacy A - Instance 1** (tenant_a1): Schema with data_1, **data_2**, data_3
- **Legacy A - Instance 2** (tenant_a2): Schema with data_1, **data_2**, data_3
- **Legacy B - Instance 1** (tenant_b1): Schema with data_a, data_b, **data_c**
- **Legacy B - Instance 2** (tenant_b2): Schema with data_a, data_b, **data_c**

**Field Mapping:**
- Legacy A instances: `data_2` → New System: `extra_field`
- Legacy B instances: `data_c` → New System: `extra_field`

**New Multi-Tenant System:**
- Unified schema with `tenant_id`
- All 4 legacy instances sync to single new database
- Each record tagged with appropriate tenant_id

## 🎨 UI Design

**Top Half (Legacy Systems):**
- Old-school gray interface
- Courier New monospace font
- Square corners, basic styling
- Looks like a 90s database application
- 4 panels showing all legacy instances

**Bottom Half (New System):**
- Ultra-modern glassmorphic design
- Dark theme with gradient backgrounds
- Smooth animations and hover effects
- Color-coded tenant badges
- Futuristic aesthetic

**Visual Separator:**
- Glowing gradient line between legacy and modern
- "⚡ MODERNIZATION LAYER ⚡" badge

## 🧪 Testing Scenarios

1. **Write to tenant_a1** → See it appear in New System with tenant_a1 badge (pink)
2. **Write to tenant_b2** → See it appear in New System with tenant_b2 badge (gray)
3. **Write via New API for tenant_a2** → Routes to Legacy A Instance 2, syncs back
4. **Watch field mapping** → data_2 and data_c both become extra_field in new system
5. **Observe CDC sync** → Changes appear in ~2 seconds after writing to legacy
6. **Test write-through** → New system writes route to correct legacy DB

## ✨ What This Demonstrates

✅ **Multiple instances per legacy system** - 2 instances of Legacy A, 2 of Legacy B  
✅ **Tenant-aware architecture** - Each instance maps to a unique tenant_id  
✅ **All fields visible** - Complete legacy schemas displayed  
✅ **Field mapping** - Different fields (data_2 vs data_c) map to same unified field  
✅ **CDC per instance** - Each instance has its own CDC triggers and changelog  
✅ **Write-through routing** - New system correctly routes writes to the right legacy instance  
✅ **Modern vs Legacy UI contrast** - Visual representation of the modernization  
✅ **Real-time sync** - Auto-refresh every 3 seconds shows CDC in action  

## 🔧 Manual Setup (Without Docker)

If you prefer to run without Docker:

```bash
# Install dependencies
pip install flask flask-cors

# Setup databases
python3 setup_legacy_a.py
python3 setup_legacy_b.py
python3 setup_new_system.py

# Run all services (use 3 terminals)
python3 sync_adapter.py     # Terminal 1
python3 api.py              # Terminal 2
python3 -m http.server 8000 # Terminal 3

# Open http://localhost:8000
```

## 📁 Files

- `Dockerfile` - Single command to run everything
- `start.sh` - Startup script that launches all services
- `setup_legacy_a.py` - Creates 2 instances of Legacy A
- `setup_legacy_b.py` - Creates 2 instances of Legacy B
- `setup_new_system.py` - Creates multi-tenant DB with 4 tenant mappings
- `sync_adapter.py` - Syncs all 4 instances to new system
- `api.py` - Flask API with tenant-aware write-through routing
- `index.html` - Modern UI with legacy/modern contrast

## 🎯 Key Architecture Patterns

### Unidirectional Sync
```
Legacy DB → CDC Trigger → Changelog → Sync Adapter → New DB
```

### Write-Through
```
New API → Write-Through Layer → Legacy DB → CDC → New DB (round-trip)
```

### Multi-Tenant Mapping
```
Legacy A Instance 1 → tenant_a1 → New System
Legacy A Instance 2 → tenant_a2 → New System
Legacy B Instance 1 → tenant_b1 → New System
Legacy B Instance 2 → tenant_b2 → New System
```

## 🛑 Stopping the Demo

If running with Docker:
```bash
# Press Ctrl+C in the terminal, or:
docker ps  # Find the container ID
docker stop <container_id>
```

## 📊 Code Stats

- **Total Lines**: ~600 lines
- **Python**: ~400 lines (backend + setup)
- **HTML/CSS/JS**: ~200 lines (frontend)
- **Demonstrates**: Complete multi-tenant modernization pattern

---

**Built to demonstrate real-world legacy modernization at scale!** 🚀

## 🐛 Troubleshooting

### No Data Showing
If you don't see any data in the UI:
1. Check the Docker logs: `docker logs <container_id>`
2. Make sure all services started successfully
3. Try refreshing the page after 5-10 seconds
4. Check browser console for any errors (F12)

### Data Not Saving
If writes aren't persisting:
1. Check that the API is accessible: `curl http://localhost:5000/api/health`
2. Look at browser network tab (F12) to see if POST requests are succeeding
3. Check Docker logs for any errors in the API or sync adapter

### Character Encoding Issues
All emoji characters have been replaced with ASCII equivalents to ensure compatibility across all systems.

### Manual Testing
You can test the API directly:
```bash
# Check health
curl http://localhost:5000/api/health

# Get all persons
curl http://localhost:5000/api/persons

# Create a person
curl -X POST http://localhost:5000/api/persons \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"tenant_a1","firstname":"Test","surname":"User","date_of_birth":"2000-01-01","city":"TestCity","extra_field":"test123"}'
```

