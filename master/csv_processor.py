import csv
import io
import json
import hashlib
import time
from datetime import datetime

def process_csv(csv_data, task_id=None):
    """Process CSV data and return structured data"""
    try:
        # Parse CSV data
        csv_file = io.StringIO(csv_data)
        reader = csv.DictReader(csv_file)
        
        # Convert to list of dictionaries
        data = list(reader)
        
        # Basic validation
        if not data:
            return {"error": "Empty CSV data", "task_id": task_id}
        
        # Check for required columns (example)
        required_columns = ["name", "value"]  # Adjust based on your CSV structure
        for col in required_columns:
            if col not in data[0]:
                return {"error": f"Missing required column: {col}", "task_id": task_id}
        
        # Data enrichment (example)
        for row in data:
            row['processed_at'] = datetime.now().isoformat()
            
        # Return processed data
        return {
            "status": "success",
            "task_id": task_id,
            "row_count": len(data),
            "data": data,
            "processed_at": datetime.now().isoformat()
        }
    except Exception as e:
        return {"error": str(e), "task_id": task_id}

def generate_task_id(csv_data):
    """Generate a unique task ID based on content and timestamp"""
    content_hash = hashlib.md5(csv_data.encode()).hexdigest()
    timestamp = int(time.time())
    return f"{content_hash}_{timestamp}"