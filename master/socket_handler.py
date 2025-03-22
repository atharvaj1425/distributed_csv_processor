from flask_socketio import SocketIO, emit
import threading
import queue
import time

class SocketHandler:
    def __init__(self, app, cors_allowed_origins="*"):
        """Initialize the socket handler with Flask app"""
        self.socketio = SocketIO(app, cors_allowed_origins=cors_allowed_origins)
        self.clients = set()
        self.message_queue = queue.Queue()
        
        # Set up event handlers
        self.register_handlers()
        
        # Start background thread for processing queued messages
        self.thread = threading.Thread(target=self._process_queue)
        self.thread.daemon = True
        self.thread.start()
    
    def register_handlers(self):
        """Register Socket.IO event handlers"""
        @self.socketio.on('connect')
        def handle_connect():
            """Handle client connection"""
            client_id = self.socketio.request.sid
            self.clients.add(client_id)
            print(f"Client connected: {client_id}, Total clients: {len(self.clients)}")
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            """Handle client disconnection"""
            client_id = self.socketio.request.sid
            if client_id in self.clients:
                self.clients.remove(client_id)
            print(f"Client disconnected: {client_id}, Total clients: {len(self.clients)}")
        
        @self.socketio.on('request_update')
        def handle_update_request(data):
            """Handle client request for latest data"""
            client_id = self.socketio.request.sid
            print(f"Update requested by client: {client_id}")
            # Emit event with 'latest_data' signal to trigger a client-specific update
            self.socketio.emit('request_acknowledged', {'status': 'processing'}, to=client_id)
    
    def emit_update(self, event_name, data):
        """Add update to the queue for broadcasting to all clients"""
        self.message_queue.put((event_name, data))
    
    def emit_to_client(self, client_id, event_name, data):
        """Add update to the queue for a specific client"""
        self.message_queue.put((event_name, data, client_id))
    
    def _process_queue(self):
        """Background thread to process the message queue"""
        while True:
            try:
                if not self.message_queue.empty():
                    message = self.message_queue.get()
                    
                    if len(message) == 3:
                        # Message for specific client
                        event_name, data, client_id = message
                        self.socketio.emit(event_name, data, to=client_id)
                    else:
                        # Broadcast message
                        event_name, data = message
                        self.socketio.emit(event_name, data)
                    
                    self.message_queue.task_done()
                
                # Sleep to prevent CPU hogging
                time.sleep(0.01)
            except Exception as e:
                print(f"Error in socket message processing: {e}")
    
    def get_socketio(self):
        """Return the SocketIO instance"""
        return self.socketio
    
    def get_client_count(self):
        """Return the number of connected clients"""
        return len(self.clients)
    
    def shutdown(self):
        """Cleanup resources"""
        # Clear message queue
        while not self.message_queue.empty():
            self.message_queue.get()
            self.message_queue.task_done()
        
        # Wait for queue to process
        self.message_queue.join()