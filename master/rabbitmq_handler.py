import pika
import json
from csv_processor import process_csv

class RabbitMQHandler:
    def __init__(self, callback, host='localhost'):
        self.callback = callback
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        
        # Declare queues
        self.channel.queue_declare(queue='csv_tasks')
        self.channel.queue_declare(queue='processed_results')
        
        # Set up consumer
        self.channel.basic_consume(
            queue='processed_results',
            on_message_callback=self.on_message,
            auto_ack=False
        )
    
    def on_message(self, ch, method, properties, body):
        try:
            data = json.loads(body)
            self.callback(data)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Error processing message: {e}")
            # Negative acknowledgment with requeue set to True
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def publish_task(self, csv_data, task_id):
        """Publish CSV task to workers"""
        message = {
            'task_id': task_id,
            'csv_data': csv_data
        }
        self.channel.basic_publish(
            exchange='',
            routing_key='csv_tasks',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                message_id=task_id  # For deduplication
            )
        )
    
    def start_consuming(self):
        """Start consuming messages"""
        self.channel.start_consuming()
    
    def stop_consuming(self):
        """Stop consuming messages and close connection"""
        self.channel.stop_consuming()
        self.connection.close()