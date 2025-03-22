import streamlit as st
import requests
import pandas as pd
import time
import socketio
import json
import threading

# Constants
API_URL = "http://localhost:5001"
SOCKET_URL = "http://localhost:5001"
REFRESH_INTERVAL = 5  # seconds

# Initialize session state
if 'data' not in st.session_state:
    st.session_state.data = None
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'connected' not in st.session_state:
    st.session_state.connected = False

# Setup SocketIO
sio = socketio.Client()

@sio.event
def connect():
    st.session_state.connected = True
    print("Connected to server")

@sio.event
def disconnect():
    st.session_state.connected = False
    print("Disconnected from server")

@sio.on('csv_update')
def on_csv_update(data):
    print("Received update via SocketIO")
    st.session_state.data = data
    st.session_state.last_update = time.time()
    # Force Streamlit to refresh
    st.rerun()

# Connect to SocketIO server if not already connected
def ensure_socket_connection():
    if not st.session_state.connected:
        try:
            sio.connect(SOCKET_URL)
        except Exception as e:
            st.error(f"Failed to connect to server: {e}")

# Header
st.title("CSV Data Dashboard")
st.subheader("Real-time CSV Processing Monitor")

# File uploader
uploaded_file = st.file_uploader("Upload CSV file for processing", type=['csv'])
if uploaded_file is not None:
    if st.button("Process CSV"):
        # Upload to server
        files = {'file': uploaded_file}
        with st.spinner("Uploading and processing..."):
            try:
                response = requests.post(f"{API_URL}/upload", files=files)
                if response.status_code == 200:
                    result = response.json()
                    st.success(f"CSV uploaded for processing. Task ID: {result.get('task_id')}")
                else:
                    st.error(f"Error uploading CSV: {response.text}")
            except Exception as e:
                st.error(f"Connection error: {e}")

# Status indicators
col1, col2 = st.columns(2)
with col1:
    # Check server health
    try:
        health_response = requests.get(f"{API_URL}/health")
        if health_response.status_code == 200:
            st.success("Server: Online")
        else:
            st.error("Server: Offline")
    except:
        st.error("Server: Offline")

with col2:
    if st.session_state.connected:
        st.success("SocketIO: Connected")
    else:
        st.warning("SocketIO: Disconnected")
        # Try to connect
        ensure_socket_connection()

# Manual refresh button
if st.button("Refresh Data"):
    try:
        response = requests.get(f"{API_URL}/data")
        if response.status_code == 200:
            st.session_state.data = response.json()
            st.session_state.last_update = time.time()
            st.success("Data refreshed")
        else:
            st.warning("No data available yet")
    except Exception as e:
        st.error(f"Error fetching data: {e}")

# Display last update time
if st.session_state.last_update:
    st.info(f"Last updated: {time.strftime('%H:%M:%S', time.localtime(st.session_state.last_update))}")

# Display data
if st.session_state.data and 'data' in st.session_state.data:
    st.subheader("Processed CSV Data")
    
    # Show metadata
    metadata = {
        "Task ID": st.session_state.data.get("task_id", "N/A"),
        "Rows": st.session_state.data.get("row_count", 0),
        "Processed by": st.session_state.data.get("worker_id", "N/A"),
        "Processing time": f"{st.session_state.data.get('processing_time', 0):.2f} seconds",
        "Processed at": st.session_state.data.get("processed_at", "N/A")
    }
    
    st.json(metadata)
    
    # Display as table
    df = pd.DataFrame(st.session_state.data["data"])
    st.dataframe(df)
    
    # Download option
    csv = df.to_csv(index=False)
    st.download_button(
        "Download as CSV",
        csv,
        "processed_data.csv",
        "text/csv",
        key='download-csv'
    )
else:
    st.info("No data available yet. Upload a CSV file or wait for processing to complete.")

# Periodically check for updates (alternative to SocketIO)
if not st.session_state.connected:
    st.warning("Using polling for updates since SocketIO is not connected")
    # Auto-refresh every few seconds using polling
    time.sleep(1)  # Small delay
    st.rerun()

# Background SocketIO monitoring
def socket_watcher():
    while True:
        if not st.session_state.connected:
            ensure_socket_connection()
        time.sleep(5)

# Start socket watcher thread
socket_thread = threading.Thread(target=socket_watcher)
socket_thread.daemon = True
socket_thread.start()