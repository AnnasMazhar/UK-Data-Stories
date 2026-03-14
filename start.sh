#!/bin/bash
# UK Data Stories - Deployment Script

set -e

echo "🚀 Starting UK Data Stories..."

# Navigate to project
cd /home/openclaw/workspace/projects/govdatastory

# Kill existing processes
echo "Stopping existing services..."
pkill -f "streamlit.*8501" 2>/dev/null || true
pkill -f "uvicorn.*8001" 2>/dev/null || true

sleep 2

# Start FastAPI (optional - for API access)
echo "Starting FastAPI on port 8001..."
.venv/bin/python -m uvicorn api.main:app --host 0.0.0.0 --port 8001 &
API_PID=$!

# Start Streamlit
echo "Starting Streamlit on port 8501..."
.venv/bin/python -m streamlit run dashboard/app.py --server.port 8501 --server.headless true &
STREAMLIT_PID=$!

echo ""
echo "✅ Services started!"
echo "  - API: http://localhost:8001"
echo "  - Dashboard: http://localhost:8501"
echo ""
echo "API PID: $API_PID"
echo "Streamlit PID: $STREAMLIT_PID"
echo ""
echo "To stop: kill $API_PID $STREAMLIT_PID"

# Save PIDs
echo "$API_PID" > /tmp/uk-data-stories-api.pid
echo "$STREAMLIT_PID" > /tmp/uk-data-stories-streamlit.pid
