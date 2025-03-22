# Distributed CSV Processor

A scalable system for processing CSV files with distributed workers, real-time updates, and a dashboard.

## System Overview

This distributed system consists of three main components:

1. **Master Server**: Flask application that handles file uploads and coordinates tasks
2. **Worker Nodes**: Distributed processes that independently process CSV tasks
3. **Dashboard**: Streamlit interface for uploading files and visualizing processed data

Communication between components uses RabbitMQ for task distribution and Flask-SocketIO for real-time updates.

## Features

- Asynchronous CSV processing with distributed workers
- Real-time dashboard updates via SocketIO
- Message deduplication and idempotent processing
- Scalable architecture - easily add more worker nodes
- Fault-tolerant with message acknowledgment and error handling

## Setup Instructions

### Prerequisites

- Python 3.7+
- RabbitMQ Server
- pip (Python package manager)

### Installing Dependencies

```bash
# Create and activate virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install flask flask-socketio pika pandas streamlit python-socketio eventlet
```

### Configuring RabbitMQ

1. Install RabbitMQ if not already installed:
   - **Ubuntu/Debian**: `apt-get install rabbitmq-server`
   - **macOS**: `brew install rabbitmq`
   - **Windows**: Download and install from https://www.rabbitmq.com/download.html

2. Start RabbitMQ service:
   - **Linux/macOS**: `sudo service rabbitmq-server start` or `brew services start rabbitmq`
   - **Windows**: Start from Services or run `rabbitmq-server.bat` from installation directory

3. Enable the management plugin (optional for monitoring):
   ```bash
   rabbitmq-plugins enable rabbitmq_management
   # Access dashboard at http://localhost:15672/ (guest/guest)
   ```

### Running the Application

1. **Start the Master Server**:
   ```bash
   cd master
   python app.py
   ```
   The Flask server will run on http://localhost:5001

2. **Launch Worker Nodes** (in separate terminal windows):
   ```bash
   cd worker
   # Launch first worker
   python worker.py worker1
   
   # Launch additional workers as needed
   python worker.py worker2
   python worker.py worker3
   ```

3. **Start the Streamlit Dashboard**:
   ```bash
   cd dashboard
   streamlit run streamlit_app.py
   ```
   The dashboard will be available at http://localhost:8501

## Using the Application

1. Open the Streamlit dashboard in your browser (http://localhost:8501)
2. Upload a CSV file using the file uploader
3. Click "Process CSV" to send the file for processing
4. The processed results will appear automatically in the dashboard
5. You can download the processed data using the "Download as CSV" button

## Project Structure

```
distributed_csv_processor/
├── dashboard/
│   └── streamlit_app.py  # Streamlit dashboard for visualization
├── master/
│   ├── app.py            # Flask server and API endpoints
│   ├── csv_processor.py  # CSV processing logic
│   ├── rabbitmq_handler.py  # RabbitMQ client for master
│   └── socket_handler.py # SocketIO server implementation
└── worker/
    └── worker.py         # Worker node implementation
```

## Design Decisions and Trade-offs

- **Message Broker**: RabbitMQ was chosen for its reliability and support for acknowledgments, ensuring no tasks are lost even if workers fail.
- **Task Distribution**: Each worker processes one task at a time to ensure fair distribution of work.
- **Deduplication**: Implemented at both the master and worker level to prevent duplicate processing of the same task.
- **Real-time Updates**: Used SocketIO for push-based updates with fallback to polling when WebSockets are not available.
- **Scalability**: The architecture allows adding more workers without reconfiguration of the master server.

## Troubleshooting

- **No data appearing after upload**: Ensure at least one worker is running and check worker logs for errors.
- **Socket connection issues**: Verify all components are running on expected hosts/ports and accessible.
- **RabbitMQ connection errors**: Confirm RabbitMQ server is running with `rabbitmqctl status`.
