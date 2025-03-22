import unittest
import os
import sys
import time
import threading
import json
import io
import pika
import pandas as pd
import subprocess
import requests
from unittest.mock import patch

# Add parent directory to path to import project modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import components (adjust imports based on your actual code structure)
from master.app import app
from master.csv_processor import process_csv, generate_task_id
from master.rabbitmq_handler import RabbitMQHandler
from worker.worker import Worker

class IntegrationTests(unittest.TestCase):
    """Integration tests for the distributed CSV processing system"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests"""
        # Start RabbitMQ if not already running (uncomment if needed)
        # cls.rabbitmq_process = subprocess.Popen(['rabbitmq-server'], stdout=subprocess.PIPE)
        # time.sleep(5)  # Wait for RabbitMQ to start
        
        # Start Flask app in a separate thread
        def run_app():
            app.run(host='0.0.0.0', port=5001, debug=False)
        
        cls.flask_thread = threading.Thread(target=run_app)
        cls.flask_thread.daemon = True
        cls.flask_thread.start()
        
        # Start a worker in a separate process
        cls.worker_process = subprocess.Popen(['python', 'worker/worker.py', 'test-worker'])
        
        # Give services time to start
        time.sleep(3)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests are done"""
        # Terminate worker process
        if hasattr(cls, 'worker_process'):
            cls.worker_process.terminate()
            cls.worker_process.wait()
        
        # Stop RabbitMQ if we started it
        # if hasattr(cls, 'rabbitmq_process'):
        #     cls.rabbitmq_process.terminate()
    
    def test_1_upload_and_processing(self):
        """Test CSV upload and processing flow"""
        # Create test CSV data
        test_df = pd.DataFrame({
            'name': ['Product1', 'Product2', 'Product3'],
            'value': [100, 200, 300],
            'category': ['A', 'B', 'A']
        })
        csv_data = test_df.to_csv(index=False)
        
        # Upload CSV via API
        files = {'file': ('test.csv', csv_data)}
        response = requests.post('http://localhost:5001/upload', files=files)
        
        # Verify upload response
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertIn('task_id', result)
        self.assertEqual(result['status'], 'success')
        
        task_id = result['task_id']
        
        # Wait for processing to complete
        max_attempts = 10
        for attempt in range(max_attempts):
            # Try to fetch processed data
            response = requests.get('http://localhost:5001/data')
            if response.status_code == 200:
                data = response.json()
                if data.get('task_id') == task_id:
                    break
            time.sleep(1)
        else:
            self.fail("Processed data not available after multiple attempts")
        
        # Verify processing results
        self.assertEqual(data['task_id'], task_id)
        self.assertEqual(data['row_count'], 3)
        self.assertEqual(data['worker_id'], 'test-worker')
        self.assertIsNotNone(data['processing_time'])
        self.assertIsNotNone(data['processed_at'])
        
        # Verify processed data content
        self.assertEqual(len(data['data']), 3)
        for row in data['data']:
            self.assertIn('name', row)
            self.assertIn('value', row)
            self.assertIn('category', row)
    
    def test_2_server_health(self):
        """Test server health endpoint"""
        response = requests.get('http://localhost:5001/health')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['status'], 'healthy')
    
    def test_3_duplicate_detection(self):
        """Test handling of duplicate messages with same task_id"""
        # Generate a fixed task ID
        test_csv = "name,value\nItem1,100"
        fixed_task_id = "duplicate_test_task"
        
        # Create direct connection to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        
        # Publish the same message twice
        message = {
            'task_id': fixed_task_id,
            'csv_data': test_csv
        }
        
        # Send duplicate messages
        for _ in range(2):
            channel.basic_publish(
                exchange='',
                routing_key='csv_tasks',
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    message_id=fixed_task_id
                )
            )
        
        # Wait for processing
        time.sleep(3)
        
        # Query API to check if data was processed
        response = requests.get('http://localhost:5001/data')
        data = response.json()
        
        # Verify task was processed (only once)
        self.assertEqual(data['task_id'], fixed_task_id)
        
        # Clean up
        connection.close()
    
    def test_4_csv_validation(self):
        """Test CSV validation"""
        # Empty CSV case
        empty_csv = "name,value\n"
        files = {'file': ('empty.csv', empty_csv)}
        response = requests.post('http://localhost:5001/upload', files=files)
        self.assertEqual(response.status_code, 200)  # Upload should still succeed
        
        # Wait a moment
        time.sleep(2)
        
        # Get data, should not be from empty CSV
        response = requests.get('http://localhost:5001/data')
        data = response.json()
        self.assertNotEqual(data.get('row_count', 1), 0)  # Should not show 0 rows
    
    def test_5_performance(self):
        """Basic performance test with larger dataset"""
        # Generate larger CSV (500 rows)
        rows = 500
        test_data = {
            'id': list(range(1, rows+1)),
            'name': [f'Product{i}' for i in range(1, rows+1)],
            'value': [i * 10 for i in range(1, rows+1)],
            'category': ['A' if i % 2 == 0 else 'B' for i in range(1, rows+1)]
        }
        test_df = pd.DataFrame(test_data)
        csv_data = test_df.to_csv(index=False)
        
        # Measure upload and processing time
        start_time = time.time()
        
        # Upload CSV
        files = {'file': ('large_test.csv', csv_data)}
        response = requests.post('http://localhost:5001/upload', files=files)
        task_id = response.json()['task_id']
        
        # Wait for processing to complete
        max_attempts = 15
        for attempt in range(max_attempts):
            response = requests.get('http://localhost:5001/data')
            if response.status_code == 200:
                data = response.json()
                if data.get('task_id') == task_id:
                    break
            time.sleep(1)
        else:
            self.fail("Large dataset processing timed out")
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Basic performance assertion (adjust threshold as needed)
        self.assertLess(processing_time, 10, 
                         f"Processing {rows} rows took {processing_time} seconds, which exceeds threshold")
        
        # Verify row count
        self.assertEqual(data['row_count'], rows)

if __name__ == '__main__':
    unittest.main()