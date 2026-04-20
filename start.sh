#!/bin/bash

# LedgerAI v2.0 Startup Script
# Safe process management with PID tracking and venv detection

set -e  # Exit on error

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDS_FILE="$PROJECT_ROOT/.ledgerai_pids"

# Cleanup: Kill only processes started by this script (tracked in .ledgerai_pids)
cleanup_previous() {
  echo "🛑 Checking for previous LedgerAI processes..."
  if [ -f "$PIDS_FILE" ]; then
    while IFS= read -r pid; do
      if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        echo "  Stopping PID $pid..."
        kill "$pid" 2>/dev/null || true
        sleep 0.5
        kill -9 "$pid" 2>/dev/null || true
      fi
    done < "$PIDS_FILE"
    rm "$PIDS_FILE"
  fi
  sleep 1
}

# Trap cleanup only on explicit termination signals.
# Do not trap EXIT, otherwise any non-zero child exit can tear down all services.
trap "cleanup_on_exit" SIGINT SIGTERM

cleanup_on_exit() {
  echo ""
  echo "🛑 Stopping all LedgerAI services..."
  if [ -f "$PIDS_FILE" ]; then
    while IFS= read -r pid; do
      if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
      fi
    done < "$PIDS_FILE"
    rm "$PIDS_FILE"
  fi
}

# Initialize PID tracking file
> "$PIDS_FILE"

cleanup_previous

echo "🐍 Starting ML Microservice (Port 5000)..."
cd "$PROJECT_ROOT/ml-service"

# Check if venv exists; activate if present
if [ -d "venv" ] && [ -f "venv/bin/activate" ]; then
  source venv/bin/activate
  echo "  ✅ Python venv activated"
elif [ -d ".venv" ] && [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
  echo "  ✅ Python venv activated"
else
  echo "  ⚠️  No venv found; assuming Python is available globally"
fi

# Start ML service and track PID
uvicorn main:app --host 0.0.0.0 --port 5000 > ml_server.log 2>&1 &
ML_PID=$!
echo "$ML_PID" >> "$PIDS_FILE"
echo "  PID: $ML_PID"

cd "$PROJECT_ROOT"

echo "⏳ Waiting for ML Microservice to load models on port 5000..."
RETRY=0
MAX_RETRIES=20
while [ $RETRY -lt $MAX_RETRIES ]; do
  if (echo > /dev/tcp/127.0.0.1/5000) >/dev/null 2>&1; then
    echo "✅ ML Microservice loaded!"
    break
  fi
  sleep 1
  RETRY=$((RETRY+1))
done

if [ $RETRY -eq $MAX_RETRIES ]; then
  echo "⚠️  ML Microservice startup took too long, proceeding anyway..."
fi

echo "🚀 Starting Node.js Backend (Port 3000)..."
cd "$PROJECT_ROOT/backend"

# Verify node and dependencies
if ! command -v node &> /dev/null; then
  echo "❌ ERROR: Node.js not found. Please install Node.js."
  exit 1
fi

# Start backend and track PID
npx nodemon server.js >> backend_server.log 2>&1 &
BACKEND_PID=$!
echo "$BACKEND_PID" >> "$PIDS_FILE"
echo "  PID: $BACKEND_PID"

cd "$PROJECT_ROOT"

echo "💻 Starting React Frontend (Vite)..."
cd "$PROJECT_ROOT/frontend-web"

# Verify npm
if ! command -v npm &> /dev/null; then
  echo "❌ ERROR: npm not found. Please install Node.js."
  exit 1
fi

# Start frontend and track PID
npm run dev -- --host > /dev/null 2>&1 &
WEB_PID=$!
echo "$WEB_PID" >> "$PIDS_FILE"
echo "  PID: $WEB_PID"

cd "$PROJECT_ROOT"

echo "==========================================="
echo "✅ ALL SERVICES STARTING IN BACKGROUND"
echo "==========================================="
echo "📍 ML Service (Port 5000)"
echo "   PID: $ML_PID"
echo "   Log: $PROJECT_ROOT/ml-service/ml_server.log"
echo ""
echo "📍 Backend API (Port 3000)"
echo "   PID: $BACKEND_PID"
echo "   Log: $PROJECT_ROOT/backend/backend_server.log"
echo ""
echo "📍 Web Frontend (Port 5173)"
echo "   PID: $WEB_PID"
echo ""
echo "Press Ctrl+C to stop all services"
echo "==========================================="

# Keep script running until user interrupts (Ctrl+C).
# `wait` with no args waits for all child processes and avoids immediate self-exit
# when one process returns a non-zero code.
wait
