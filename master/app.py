from flask import Flask, jsonify, request
from flask_socketio import SocketIO
import threading
import json
from rabbitmq_handler import RabbitMQHandler
from csv_processor import process_csv, generate_task_id

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Shared state for most recent data
latest_data = None
processed_tasks = set()  # For deduplication

def handle_processed_result(data):
    """Handle processed results from workers"""
    global latest_data
    
    # Deduplication check
    task_id = data.get('task_id')
    if task_id in processed_tasks:
        print(f"Duplicate task detected: {task_id}")
        return
    
    processed_tasks.add(task_id)
    # Limit size of processed_tasks set
    if len(processed_tasks) > 1000:
        processed_tasks.pop()
    
    # Update latest data
    latest_data = data
    
    # Emit SocketIO event
    socketio.emit('csv_update', data)

# Initialize RabbitMQ handler
rabbit_handler = RabbitMQHandler(callback=handle_processed_result)

# Start RabbitMQ consumer in a separate thread
def start_rabbitmq_consumer():
    rabbit_handler.start_consuming()

consumer_thread = threading.Thread(target=start_rabbitmq_consumer)
consumer_thread.daemon = True
consumer_thread.start()

@app.route('/data', methods=['GET'])
def get_data():
    """Return the most recently processed CSV data"""
    if latest_data is None:
        return jsonify({"error": "No data available yet"}), 404
    return jsonify(latest_data)

@app.route('/upload', methods=['POST'])
def upload_csv():
    """Upload CSV for processing"""
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    try:
        # Read CSV content
        csv_content = file.read().decode('utf-8')
        
        # Generate task ID
        task_id = generate_task_id(csv_content)
        
        # Publish to RabbitMQ
        rabbit_handler.publish_task(csv_content, task_id)
        
        return jsonify({
            "status": "success", 
            "message": "CSV uploaded for processing",
            "task_id": task_id
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy"})

@socketio.on('connect')
def handle_connect():
    """Handle client connection""" 
    print("Client connected")
    if latest_data:
        socketio.emit('csv_update', latest_data, to=request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print("Client disconnected")

@socketio.on('request_update')
def handle_update_request(data):
    """Handle manual update request from client"""
    if latest_data:
        socketio.emit('csv_update', latest_data, to=request.sid)

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5001)