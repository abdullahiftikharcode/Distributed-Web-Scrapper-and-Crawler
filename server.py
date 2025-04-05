import socket
import threading
import json
import time
import logging
import pymongo
import pika
import os
import yaml
from datetime import datetime
from queue import Queue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CrawlerServer:
    def __init__(self, host='0.0.0.0', port=5555):
        self.host = host
        self.port = port
        self.worker_registry = {}  # Dictionary to track active workers
        self.worker_lock = threading.Lock()  # Lock for worker_registry access
        self.server_socket = None
        self.running = False
        self.last_heartbeat_check = time.time()
        
        # Load configuration
        try:
            with open('config.yaml', 'r') as f:
                self.config = yaml.safe_load(f)
            logger.info("Successfully loaded configuration")
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise
        
        # Connect to MongoDB
        try:
            mongo_uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
            self.mongo_client = pymongo.MongoClient(mongo_uri)
            self.db = self.mongo_client["web_crawler"]
            self.url_queue = self.db["url_queue"]
            self.visited_urls = self.db["visited_urls"]
            self.pages = self.db["pages"]
            self.worker_status = self.db["worker_status"]
            logger.info("Successfully connected to MongoDB")
            
            # Create indexes
            self.worker_status.create_index("worker_id", unique=True)
            logger.info("Created MongoDB indexes")
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {str(e)}")
            raise
        
        # Connect to RabbitMQ
        try:
            rabbit_host = os.environ.get("RABBITMQ_HOST", "localhost")
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue='url_queue', durable=True)
            logger.info("Successfully connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Error connecting to RabbitMQ: {str(e)}")
            raise
    
    def start(self):
        """Start the server and listen for incoming connections"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            logger.info(f"Server started on {self.host}:{self.port}")
            
            # Start heartbeat checker thread
            heartbeat_thread = threading.Thread(target=self.check_worker_heartbeats)
            heartbeat_thread.daemon = True
            heartbeat_thread.start()
            
            # Main server loop
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    logger.info(f"New connection from {address}")
                    client_handler = threading.Thread(target=self.handle_client, args=(client_socket, address))
                    client_handler.daemon = True
                    client_handler.start()
                except Exception as e:
                    if self.running:
                        logger.error(f"Error accepting connection: {str(e)}")
                        time.sleep(1)
        except Exception as e:
            logger.error(f"Error starting server: {str(e)}")
        finally:
            self.shutdown()
    
    def check_worker_heartbeats(self):
        """Periodically check worker heartbeats and remove stale connections"""
        while self.running:
            try:
                current_time = time.time()
                timeout_threshold = current_time - 60  # 60 seconds timeout
                
                with self.worker_lock:
                    # Find workers with stale heartbeats
                    stale_workers = []
                    for worker_id, worker_info in self.worker_registry.items():
                        if worker_info['last_heartbeat'] < timeout_threshold:
                            stale_workers.append(worker_id)
                    
                    # Remove stale workers
                    for worker_id in stale_workers:
                        worker_info = self.worker_registry.pop(worker_id)
                        try:
                            worker_info['socket'].close()
                        except:
                            pass
                        logger.warning(f"Removed stale worker: {worker_id}")
                        
                        # Update worker status in MongoDB
                        self.worker_status.update_one(
                            {"worker_id": worker_id},
                            {"$set": {
                                "status": "disconnected",
                                "last_activity": current_time,
                                "disconnected_at": current_time
                            }},
                            upsert=True
                        )
            except Exception as e:
                logger.error(f"Error in heartbeat checker: {str(e)}")
            
            time.sleep(30)  # Check every 30 seconds
    
    def handle_client(self, client_socket, address):
        """Handle a client connection"""
        worker_id = None
        try:
            # Receive initial registration message
            data = self.receive_data(client_socket)
            if not data or 'type' not in data or data['type'] != 'register':
                logger.warning(f"Invalid registration from {address}")
                client_socket.close()
                return
            
            worker_id = data.get('worker_id')
            if not worker_id:
                logger.warning(f"Missing worker_id in registration from {address}")
                client_socket.close()
                return
            
            # Register worker
            worker_info = {
                'socket': client_socket,
                'address': address,
                'last_heartbeat': time.time(),
                'status': 'connected',
                'current_task': None
            }
            
            with self.worker_lock:
                # Check if worker_id already exists
                if worker_id in self.worker_registry:
                    logger.warning(f"Worker {worker_id} already registered")
                    old_socket = self.worker_registry[worker_id]['socket']
                    try:
                        old_socket.close()
                    except:
                        pass
                
                self.worker_registry[worker_id] = worker_info
            
            # Update worker status in MongoDB
            self.worker_status.update_one(
                {"worker_id": worker_id},
                {"$set": {
                    "status": "connected",
                    "last_activity": time.time(),
                    "connected_at": time.time(),
                    "address": f"{address[0]}:{address[1]}"
                }},
                upsert=True
            )
            
            logger.info(f"Worker {worker_id} registered from {address}")
            
            # Send acknowledgment with configuration
            response = {
                'type': 'register_ack',
                'status': 'success',
                'config': self.config
            }
            self.send_data(client_socket, response)
            
            # Main client loop (handle messages)
            while self.running:
                try:
                    data = self.receive_data(client_socket)
                    if not data:
                        logger.warning(f"Connection closed by worker {worker_id}")
                        break
                    
                    # Handle different message types
                    if data['type'] == 'heartbeat':
                        # Update heartbeat timestamp
                        with self.worker_lock:
                            if worker_id in self.worker_registry:
                                self.worker_registry[worker_id]['last_heartbeat'] = time.time()
                        
                        # Update worker status in MongoDB
                        self.worker_status.update_one(
                            {"worker_id": worker_id},
                            {"$set": {
                                "last_activity": time.time(),
                                "status": "active"
                            }}
                        )
                        
                        # Send heartbeat acknowledgment
                        response = {'type': 'heartbeat_ack'}
                        self.send_data(client_socket, response)
                    
                    elif data['type'] == 'request_task':
                        # Worker is requesting a new task
                        with self.worker_lock:
                            if worker_id in self.worker_registry:
                                self.worker_registry[worker_id]['status'] = 'waiting'
                        
                        # Get a URL from the queue
                        next_url, depth = self.get_next_url()
                        
                        if next_url:
                            # Update worker status
                            with self.worker_lock:
                                if worker_id in self.worker_registry:
                                    self.worker_registry[worker_id]['status'] = 'working'
                                    self.worker_registry[worker_id]['current_task'] = next_url
                            
                            # Update worker status in MongoDB
                            self.worker_status.update_one(
                                {"worker_id": worker_id},
                                {"$set": {
                                    "status": "working",
                                    "current_url": next_url,
                                    "last_activity": time.time()
                                }}
                            )
                            
                            # Send task to worker
                            response = {
                                'type': 'task',
                                'url': next_url,
                                'depth': depth
                            }
                        else:
                            # No tasks available
                            response = {
                                'type': 'no_task'
                            }
                            
                            # Update worker status
                            with self.worker_lock:
                                if worker_id in self.worker_registry:
                                    self.worker_registry[worker_id]['status'] = 'idle'
                            
                            # Update worker status in MongoDB
                            self.worker_status.update_one(
                                {"worker_id": worker_id},
                                {"$set": {
                                    "status": "idle",
                                    "last_activity": time.time()
                                }}
                            )
                        
                        self.send_data(client_socket, response)
                    
                    elif data['type'] == 'task_complete':
                        # Worker has completed a task
                        url = data.get('url')
                        success = data.get('success', False)
                        links = data.get('links', [])
                        page_data = data.get('data', {})
                        
                        # Update worker status
                        with self.worker_lock:
                            if worker_id in self.worker_registry:
                                self.worker_registry[worker_id]['status'] = 'idle'
                                self.worker_registry[worker_id]['current_task'] = None
                        
                        # Update worker status in MongoDB
                        self.worker_status.update_one(
                            {"worker_id": worker_id},
                            {"$set": {
                                "status": "idle",
                                "current_url": None,
                                "last_completed_url": url,
                                "last_activity": time.time()
                            }}
                        )
                        
                        # Update URL status in queue
                        if success:
                            self.url_queue.update_one(
                                {"url": url},
                                {"$set": {
                                    "status": "completed",
                                    "completed_at": time.time(),
                                    "worker_id": worker_id
                                }}
                            )
                            
                            # Store page data if successful
                            if page_data:
                                self.store_page_data(url, worker_id, page_data)
                            
                            # Add discovered links to queue
                            if links:
                                for link_data in links:
                                    self.add_url_to_queue(link_data['url'], link_data['depth'], worker_id)
                        else:
                            self.url_queue.update_one(
                                {"url": url},
                                {"$set": {
                                    "status": "failed",
                                    "failed_at": time.time(),
                                    "worker_id": worker_id
                                }}
                            )
                        
                        # Send acknowledgment
                        response = {'type': 'task_complete_ack'}
                        self.send_data(client_socket, response)
                    
                    elif data['type'] == 'shutdown':
                        # Worker is shutting down
                        logger.info(f"Worker {worker_id} shutting down")
                        break
                    
                    else:
                        logger.warning(f"Unknown message type from worker {worker_id}: {data['type']}")
                
                except Exception as e:
                    logger.error(f"Error handling message from worker {worker_id}: {str(e)}")
                    break
        
        except Exception as e:
            logger.error(f"Error handling client {address}: {str(e)}")
        
        finally:
            # Clean up connection
            try:
                client_socket.close()
            except:
                pass
            
            # Remove worker from registry
            with self.worker_lock:
                if worker_id and worker_id in self.worker_registry:
                    self.worker_registry.pop(worker_id)
            
            # Update worker status in MongoDB
            if worker_id:
                self.worker_status.update_one(
                    {"worker_id": worker_id},
                    {"$set": {
                        "status": "disconnected",
                        "last_activity": time.time(),
                        "disconnected_at": time.time()
                    }},
                    upsert=True
                )
                
            logger.info(f"Connection closed with {address} (Worker ID: {worker_id})")
    
    def receive_data(self, client_socket):
        """Receive and parse JSON data from a socket"""
        try:
            # Read message length (4 bytes)
            length_data = client_socket.recv(4)
            if not length_data:
                return None
            
            message_length = int.from_bytes(length_data, byteorder='big')
            
            # Read the actual message
            data = b''
            while len(data) < message_length:
                chunk = client_socket.recv(min(4096, message_length - len(data)))
                if not chunk:
                    return None
                data += chunk
            
            # Parse JSON data
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Error receiving data: {str(e)}")
            return None
    
    def send_data(self, client_socket, data):
        """Encode and send JSON data to a socket"""
        try:
            # Convert data to JSON and encode
            json_data = json.dumps(data).encode('utf-8')
            
            # Send message length (4 bytes) followed by the data
            message_length = len(json_data).to_bytes(4, byteorder='big')
            client_socket.sendall(message_length + json_data)
            return True
        except Exception as e:
            logger.error(f"Error sending data: {str(e)}")
            return False
    
    def get_next_url(self):
        """Get next URL from queue"""
        try:
            # Use findOneAndUpdate for atomic operation
            result = self.url_queue.find_one_and_update(
                {"status": "pending"},
                {"$set": {"status": "processing", "processing_started": time.time()}},
                sort=[("timestamp", 1)],
                return_document=pymongo.ReturnDocument.AFTER
            )
            
            if result:
                logger.info(f"Got next URL: {result['url']}")
                return result["url"], result.get("depth", 0)
            logger.debug("No URLs in queue")
            return None, None
        except Exception as e:
            logger.error(f"Error getting next URL: {str(e)}")
            return None, None
    
    def add_url_to_queue(self, url, depth, worker_id):
        """Add URL to queue if it hasn't been visited and is allowed"""
        try:
            # Check if URL is already in queue with any status
            existing = self.url_queue.find_one({"url": url})
            if existing:
                logger.debug(f"URL already in queue: {url}")
                return False
                
            # Check if URL has been visited
            visited = self.visited_urls.find_one({"url": url})
            if visited:
                logger.debug(f"URL already visited: {url}")
                return False
            
            # Add URL to queue with pending status
            self.url_queue.insert_one({
                "url": url,
                "depth": depth,
                "timestamp": time.time(),
                "status": "pending",
                "added_by": worker_id
            })
            
            # Publish to RabbitMQ
            message = {
                "url": url,
                "depth": depth,
                "timestamp": time.time()
            }
            self.channel.basic_publish(
                exchange='',
                routing_key='url_queue',
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type='application/json'
                )
            )
            
            logger.info(f"Added URL to queue: {url}")
            return True
        except Exception as e:
            logger.error(f"Error adding URL to queue: {str(e)}")
            return False
    
    def store_page_data(self, url, worker_id, data):
        """Store page data in MongoDB"""
        try:
            # Add metadata
            data['url'] = url
            data['timestamp'] = time.time()
            data['worker_id'] = worker_id
            
            # Store in MongoDB
            self.pages.insert_one(data)
            
            # Mark URL as visited
            self.visited_urls.insert_one({
                "url": url,
                "worker_id": worker_id,
                "timestamp": time.time()
            })
            
            logger.info(f"Stored data for URL: {url}")
            return True
        except Exception as e:
            logger.error(f"Error storing page data: {str(e)}")
            return False
    
    def get_worker_status(self):
        """Get a list of all active workers and their statuses"""
        try:
            with self.worker_lock:
                worker_list = []
                for worker_id, worker_info in self.worker_registry.items():
                    worker_list.append({
                        'worker_id': worker_id,
                        'address': worker_info['address'],
                        'status': worker_info['status'],
                        'last_heartbeat': worker_info['last_heartbeat'],
                        'current_task': worker_info['current_task']
                    })
                return worker_list
        except Exception as e:
            logger.error(f"Error getting worker status: {str(e)}")
            return []
    
    def shutdown(self):
        """Shutdown the server"""
        logger.info("Shutting down server...")
        self.running = False
        
        # Close all worker connections
        with self.worker_lock:
            for worker_id, worker_info in list(self.worker_registry.items()):
                try:
                    worker_info['socket'].close()
                except:
                    pass
            self.worker_registry.clear()
        
        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        # Close MongoDB and RabbitMQ connections
        try:
            self.mongo_client.close()
        except:
            pass
        
        try:
            self.connection.close()
        except:
            pass
        
        logger.info("Server shutdown complete")

if __name__ == "__main__":
    server = CrawlerServer()
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Server interrupted. Shutting down...")
    finally:
        server.shutdown() 