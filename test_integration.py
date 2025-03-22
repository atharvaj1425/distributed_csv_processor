import unittest
import os
import time
import csv
import io
import json
import requests
import threading
import socketio
import pika
import pandas as pd
import tempfile
import sys
from pathlib import Path

# Add project root to path for importing modules
sys.path.insert(0, str(Path(__file__).parent))

# Import modules from project
from master.csv_processor import process_csv, generate_task_id
from master.rabbitmq_handler import RabbitMQHandler
from worker.worker import Worker

class DistributedCSVProcessorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment once before all tests"""
        # Create test data
        cls.test_csv_content = "name,value,category\nitem1,10,A\nitem2,20,B\nitem3,30,C"
        cls.test_csv_path = cls._create_test_csv()
        
        # API endpoints
        cls.API_URL = "http://localhost:5001"
        cls.SOCKET_URL = "http://localhost:5001"
        
        # Check if services are running
        cls._check_services_running()
        
        # Setup SocketIO for receiving updates
        cls.received_updates = []
        cls.sio = socketio.Client()
        
        @cls.sio.event
        def connect():
            print("SocketIO connected")
        
        @cls.sio.on('csv_update')
        def on_csv_update(data):
            print("Received update via SocketIO")
            cls.received_updates.append(data)
        
        # Connect to SocketIO
        try:
            cls.sio.connect(cls.SOCKET_URL)
        except Exception as e:
            print(f"Warning: Could not connect to SocketIO server: {e}")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests are done"""
        # Disconnect SocketIO
        if cls.sio.connected:
            cls.sio.disconnect()
        
        # Clean up test file
        if os.path.exists(cls.test_csv_path):
            os.remove(cls.test_csv_path)
    
    @classmethod
    def _create_test_csv(cls):
        """Create a test CSV file"""
        fd, path = tempfile.mkstemp(suffix='.csv')
        with os.fdopen(fd, 'w') as f:
            f.write(cls.test_csv_content)
        return path
    
    @classmethod
    def _check_services_running(cls):
        """Verify all required services are running"""
        # Check Flask server
        try:
            response = requests.get(f"{cls.API_URL}/health")
            if response.status_code != 200:
                print("Warning: Master server is not responding correctly")
        except requests.exceptions.ConnectionError:
            print("Warning: Master server is not running")
        
        # Check RabbitMQ
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            connection.close()
        except Exception:
            print("Warning: RabbitMQ server is not running")
    
    def test_01_csv_processor(self):
        """Test CSV processing functionality"""
        # Test CSV processing
        result = process_csv(self.test_csv_content)
        
        # Verify result structure
        self.assertIn('status', result)
        self.assertEqual(result['status'], 'success')
        self.assertIn('row_count', result)
        self.assertEqual(result['row_count'], 3)
        self.assertIn('data', result)
        
        # Check data content
        data = result['data']
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]['name'], 'item1')
        self.assertEqual(data[1]['value'], '20')
        self.assertEqual(data[2]['category'], 'C')
    
    def test_02_task_id_generation(self):
        """Test task ID generation"""
        # Generate task IDs
        task_id1 = generate_task_id(self.test_csv_content)
        task_id2 = generate_task_id(self.test_csv_content)
        task_id3 = generate_task_id("different,content\n1,2")
        
        # Task IDs for the same content should have the same hash prefix
        self.assertTrue(task_id1.split('_')[0] == task_id2.split('_')[0])
        
        # Task IDs for different content should have different hash prefixes
        self.assertFalse(task_id1.split('_')[0] == task_id3.split('_')[0])
    
    def test_03_rabbitmq_handler(self):
        """Test RabbitMQ handler functionality"""
        # Create a test callback function
        callback_data = []
        
        def test_callback(data):
            callback_data.append(data)
        
        # Initialize RabbitMQ handler
        handler = RabbitMQHandler(callback=test_callback)
        
        # Generate a unique task ID
        task_id = f"test_{int(time.time())}"
        
        # Publish a test task
        handler.publish_task(self.test_csv_content, task_id)
        
        # Start consuming in a separate thread
        consumer_thread = threading.Thread(target=handler.start_consuming)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Wait for processing
        timeout = 10
        start_time = time.time()
        while not callback_data and time.time() - start_time < timeout:
            time.sleep(0.5)
        
        # Stop consuming
        handler.stop_consuming()
        
        # Check if we received a callback
        self.assertTrue(len(callback_data) > 0, "No callback data received from RabbitMQ")
    
    def test_04_worker_functionality(self):
        """Test worker node functionality"""
        # Initialize a worker in a separate thread
        worker_id = f"test-worker-{int(time.time())}"
        worker = Worker(worker_id=worker_id)
        
        # Mock a task message
        task_id = f"test_task_{int(time.time())}"
        message = {
            'task_id': task_id,
            'csv_data': self.test_csv_content
        }
        
        # Create a channel for publishing
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        
        # Publish a test task
        channel.basic_publish(
            exchange='',
            routing_key='csv_tasks',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,
                message_id=task_id
            )
        )
        
        # Start worker in a separate thread
        worker_thread = threading.Thread(target=worker.start)
        worker_thread.daemon = True
        worker_thread.start()
        
        # Wait for processing
        time.sleep(3)
        
        # Check if a result was published
        channel.queue_declare(queue='processed_results')
        method_frame, header_frame, body = channel.basic_get(queue='processed_results')
        
        # Cleanup
        if method_frame:
            channel.basic_ack(method_frame.delivery_tag)
        connection.close()
        
        # Verify we got a result
        self.assertIsNotNone(method_frame, "No result was published by the worker")
        
        # Parse and verify result
        if body:
            result = json.loads(body)
            self.assertEqual(result['worker_id'], worker_id)
            self.assertEqual(result['task_id'], task_id)
            self.assertEqual(result['row_count'], 3)
    
    def test_05_flask_endpoints(self):
        """Test Flask API endpoints"""
        # Test health endpoint
        health_response = requests.get(f"{self.API_URL}/health")
        self.assertEqual(health_response.status_code, 200)
        self.assertEqual(health_response.json()['status'], 'healthy')
        
        # Test file upload
        with open(self.test_csv_path, 'rb') as f:
            files = {'file': f}
            upload_response = requests.post(f"{self.API_URL}/upload", files=files)
        
        self.assertEqual(upload_response.status_code, 200)
        upload_result = upload_response.json()
        self.assertEqual(upload_result['status'], 'success')
        self.assertIn('task_id', upload_result)
        
        # Wait for processing
        time.sleep(3)
        
        # Test data retrieval
        data_response = requests.get(f"{self.API_URL}/data")
        self.assertEqual(data_response.status_code, 200)
        data = data_response.json()
        self.assertIn('data', data)
        self.assertIn('task_id', data)
        self.assertIn('row_count', data)
    
    def test_06_socketio_updates(self):
        """Test SocketIO event broadcasting"""
        # Clear existing updates
        self.received_updates.clear()
        
        # Upload a new file to trigger updates
        with open(self.test_csv_path, 'rb') as f:
            files = {'file': f}
            requests.post(f"{self.API_URL}/upload", files=files)
        
        # Wait for SocketIO update
        timeout = 10
        start_time = time.time()
        while not self.received_updates and time.time() - start_time < timeout:
            time.sleep(0.5)
        
        # Verify we received an update
        self.assertTrue(len(self.received_updates) > 0, "No SocketIO updates received")
        
        # Check update content
        update = self.received_updates[0]
        self.assertIn('data', update)
        self.assertIn('task_id', update)
        self.assertIn('worker_id', update)
    
    def test_07_end_to_end_flow(self):
        """Test end-to-end data flow"""
        # Clear existing updates
        self.received_updates.clear()
        
        # 1. Create a unique test file
        unique_content = f"name,value,created\ntest,123,{time.time()}"
        fd, unique_path = tempfile.mkstemp(suffix='.csv')
        with os.fdopen(fd, 'w') as f:
            f.write(unique_content)
        
        # 2. Upload the file
        with open(unique_path, 'rb') as f:
            files = {'file': f}
            upload_response = requests.post(f"{self.API_URL}/upload", files=files)
        
        # Get the task ID
        task_id = upload_response.json()['task_id']
        
        # 3. Wait for processing
        timeout = 15
        start_time = time.time()
        processed_data = None
        
        while time.time() - start_time < timeout:
            # Check for SocketIO updates
            if self.received_updates:
                for update in self.received_updates:
                    if update.get('task_id') == task_id:
                        processed_data = update
                        break
            
            # If no SocketIO update, try the API
            if not processed_data:
                try:
                    response = requests.get(f"{self.API_URL}/data")
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('task_id') == task_id:
                            processed_data = data
                except:
                    pass
            
            if processed_data:
                break
                
            time.sleep(1)
        
        # Clean up
        os.remove(unique_path)
        
        # 4. Verify processing
        self.assertIsNotNone(processed_data, f"Task {task_id} was not processed within timeout")
        
        # 5. Check content
        self.assertEqual(processed_data['row_count'], 1)
        self.assertEqual(processed_data['data'][0]['name'], 'test')
        self.assertEqual(processed_data['data'][0]['value'], '123')
        self.assertIn('worker_id', processed_data)
        self.assertIn('processing_time', processed_data)

if __name__ == '__main__':
    unittest.main(verbosity=2)
