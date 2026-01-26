#!/bin/bash

echo "🚀 Starting Legacy Modernization PoC (Docker Multi-Container)"
echo "============================================================="
echo ""

# Check if docker-compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Error: docker-compose is not installed"
    echo "Please install Docker and Docker Compose first"
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Error: Docker is not running"
    echo "Please start Docker first"
    exit 1
fi

echo "📦 Building images..."
docker-compose build

echo ""
echo "🏗️  Starting all services..."
docker-compose up -d

echo ""
echo "⏳ Waiting for services to be ready..."
echo "   This may take 30-60 seconds on first startup..."
echo ""

# Wait for API to be ready
echo "   Waiting for API service..."
until curl -s http://localhost:5000/api/health > /dev/null 2>&1; do
    sleep 2
done
echo "   ✅ API ready"

# Wait for Debezium to be ready
echo "   Waiting for Debezium..."
until curl -s http://localhost:8083/ > /dev/null 2>&1; do
    sleep 2
done
echo "   ✅ Debezium ready"

# Give CDC consumer time to connect
echo "   Waiting for CDC Consumer..."
sleep 10
echo "   ✅ CDC Consumer should be running"

echo ""
echo "✅ All services are up!"
echo ""
echo "📊 Service Status:"
docker-compose ps
echo ""
echo "🌐 Access Points:"
echo "   • Web UI:          http://localhost:8000"
echo "   • API:             http://localhost:5000/api/health"
echo "   • Debezium API:    http://localhost:8083/connectors"
echo ""
echo "📝 Useful Commands:"
echo "   • View all logs:       docker-compose logs -f"
echo "   • View consumer logs:  docker-compose logs -f cdc-consumer"
echo "   • Stop all services:   docker-compose down"
echo "   • Restart service:     docker-compose restart <service>"
echo ""
echo "📖 See README-DOCKER.md for full documentation"
echo ""
