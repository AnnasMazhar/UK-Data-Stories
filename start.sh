#!/bin/bash
# Start GovDataStory services

echo "Starting GovDataStory..."

# Kill existing processes on our ports
pkill -f "uvicorn api.main" 2>/dev/null || true
pkill -f "streamlit run" 2>/dev/null || true

sleep 1

# Start FastAPI on port 8001
cd /home/openclaw/workspace/projects/govdatastory
.venv/bin/python -m uvicorn api.main:app --host 0.0.0.0 --port 8001 &
API_PID=$!

echo "FastAPI started on port 8001 (PID: $API_PID)"

# Wait a moment
sleep 2

# Start Streamlit on port 8501
.venv/bin/python -m streamlit run dashboard/app.py --server.port 8501 &
STREAMLIT_PID=$!

echo "Streamlit started on port 8501 (PID: $STREAMLIT_PID)"
echo ""
echo "GovDataStory is running!"
echo "  - API: http://localhost:8001"
echo "  - Dashboard: http://localhost:8501"
echo ""
echo "To stop:"
echo "  kill $API_PID $STREAMLIT_PID"
