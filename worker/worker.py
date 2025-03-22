import pika
import json
import sys
import time
import os
import random
from csv_processor import process_csv

class Worker:
    def __init__(self, worker_id, host='localhost'):
        self.worker_id = worker_id
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        
        # Declare queues
        self.channel.queue_declare(queue='csv_tasks')
        self.channel.queue_declare(queue='processed_results')
        
        # Set prefetch count to limit concurrent tasks
        self.channel.basic_qos(prefetch_count=1)
        
        # Set up consumer with manual acknowledgment
        self.channel.basic_consume(
            queue='csv_tasks',
            on_message_callback=self.process_task,
            auto_ack=False
        )
        
        # Track processed tasks to avoid duplicates
        self.processed_tasks = set()
        
    def process_task(self, ch, method, properties, body):
        """Process CSV task and publish result"""
        try:
            # Parse message
            message = json.loads(body)
            task_id = message.get('task_id')
            csv_data = message.get('csv_data')
            
            # Check for duplicates
            if task_id in self.processed_tasks:
                print(f"Worker {self.worker_id}: Skipping duplicate task {task_id}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            print(f"Worker {self.worker_id}: Processing task {task_id}")
            
            # Simulate processing time
            processing_time = random.uniform(0.5, 2.0)
            time.sleep(processing_time)
            
            # Process CSV data
            result = process_csv(csv_data, task_id)
            result['worker_id'] = self.worker_id
            result['processing_time'] = processing_time
            
            # Add to processed tasks
            self.processed_tasks.add(task_id)
            # Limit size of set
            if len(self.processed_tasks) > 100:
                self.processed_tasks.pop()
            
            # Publish result
            self.channel.basic_publish(
                exchange='',
                routing_key='processed_results',
                body=json.dumps(result),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    message_id=task_id  # For deduplication
                )
            )
            
            # Acknowledge message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            print(f"Worker {self.worker_id}: Completed task {task_id}")
            
        except Exception as e:
            print(f"Worker {self.worker_id}: Error processing task: {e}")
            # Negative acknowledgment with requeue
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def start(self):
        """Start the worker"""
        print(f"Worker {self.worker_id} started. Waiting for CSV tasks...")
        self.channel.start_consuming()

if __name__ == '__main__':
    # Use command line arg as worker ID or generate a random one
    worker_id = sys.argv[1] if len(sys.argv) > 1 else f"worker-{random.randint(1000, 9999)}"
    
    # Add random delay to reduce chance of multiple workers starting simultaneously
    time.sleep(random.uniform(0.1, 0.5))
    
    worker = Worker(worker_id)
    
    try:
        worker.start()
    except KeyboardInterrupt:
        print(f"Worker {worker_id} stopped")
        sys.exit(0)