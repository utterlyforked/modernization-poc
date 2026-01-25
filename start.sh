#!/bin/bash

echo "🚀 Legacy Modernization Demo - Starting..."
echo "=========================================="

# Setup databases
echo "📦 Setting up databases..."
python3 setup_legacy_a.py
python3 setup_legacy_b.py
python3 setup_new_system.py

echo ""
echo "✅ Databases ready!"
echo ""

# Start sync adapter in background
echo "🔄 Starting sync adapter..."
python3 sync_adapter.py &
SYNC_PID=$!

# Give sync adapter a moment to start
sleep 2

# Start API server in background
echo "🌐 Starting API server..."
python3 api.py &
API_PID=$!

# Give API a moment to start
sleep 2

# Start HTTP server for frontend
echo "🎨 Starting web server..."
python3 -m http.server 8000 &
HTTP_PID=$!

echo ""
echo "✅ All services running!"
echo ""
echo "📍 Access the demo at: http://localhost:8000"
echo ""
echo "Services:"
echo "  - Sync Adapter: Running (PID: $SYNC_PID)"
echo "  - API Server: http://localhost:5000 (PID: $API_PID)"
echo "  - Web UI: http://localhost:8000 (PID: $HTTP_PID)"
echo ""
echo "Press Ctrl+C to stop all services"

# Wait for any process to exit
wait -n

# Kill all background processes
kill $SYNC_PID $API_PID $HTTP_PID 2>/dev/null

echo ""
echo "🛑 Services stopped"
