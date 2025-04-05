import os
import logging
import socket
import json
import time
import threading
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import uuid
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Hardcoded server configuration
SERVER_HOST = "127.0.0.1"  # Native machine IP address
SERVER_PORT = 5555

class RemoteWorker:
    def __init__(self, worker_id=None):
        self.server_host = SERVER_HOST
        self.server_port = SERVER_PORT
        self.worker_id = worker_id or f"worker-{uuid.uuid4()}"
        self.socket = None
        self.connected = False
        self.running = False
        self.config = None
        self.heartbeat_thread = None
        
        logger.info(f"Initializing worker {self.worker_id}")
    
    def connect(self):
        """Connect to the central server"""
        try:
            logger.info(f"Connecting to server at {self.server_host}:{self.server_port}")
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.server_host, self.server_port))
            self.connected = True
            logger.info("Successfully connected to server")
            
            # Register with server
            registration = {
                'type': 'register',
                'worker_id': self.worker_id
            }
            if self.send_data(registration):
                logger.info("Sent registration message")
                
                # Wait for acknowledgment
                response = self.receive_data()
                if response and response.get('type') == 'register_ack':
                    logger.info("Registration acknowledged by server")
                    self.config = response.get('config', {})
                    return True
                else:
                    logger.error("Failed to receive registration acknowledgment")
            else:
                logger.error("Failed to send registration message")
            
            self.disconnect()
            return False
        except Exception as e:
            logger.error(f"Error connecting to server: {str(e)}")
            self.disconnect()
            return False
    
    def disconnect(self):
        """Disconnect from the server"""
        self.connected = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None
        logger.info("Disconnected from server")
    
    def start(self):
        """Start the worker"""
        if not self.connect():
            logger.error("Failed to connect to server. Worker cannot start.")
            return False
        
        self.running = True
        logger.info(f"Worker {self.worker_id} started")
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeats)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        # Main worker loop
        try:
            while self.running and self.connected:
                # Request a task from the server
                request = {'type': 'request_task'}
                if not self.send_data(request):
                    logger.error("Failed to request task from server")
                    break
                
                # Wait for response
                response = self.receive_data()
                if not response:
                    logger.error("Failed to receive response from server")
                    break
                
                # Handle response
                if response.get('type') == 'task':
                    # Process the task
                    url = response.get('url')
                    depth = response.get('depth', 0)
                    logger.info(f"Received task: {url} (depth: {depth})")
                    
                    result = self.process_url(url, depth)
                    
                    # Send result back to server
                    task_complete = {
                        'type': 'task_complete',
                        'url': url,
                        'success': result['success'],
                        'data': result.get('data', {}),
                        'links': result.get('links', [])
                    }
                    if not self.send_data(task_complete):
                        logger.error("Failed to send task completion status")
                        break
                    
                    # Wait for acknowledgment
                    ack = self.receive_data()
                    if not ack or ack.get('type') != 'task_complete_ack':
                        logger.error("Failed to receive task completion acknowledgment")
                        break
                    
                elif response.get('type') == 'no_task':
                    logger.info("No tasks available. Waiting...")
                    time.sleep(5)  # Wait before requesting again
                
                else:
                    logger.warning(f"Unknown response type: {response.get('type')}")
                    time.sleep(1)
        
        except Exception as e:
            logger.error(f"Error in worker main loop: {str(e)}")
        
        finally:
            self.shutdown()
    
    def process_url(self, url, depth):
        """Process a single URL and extract data"""
        try:
            logger.info(f"Processing URL: {url}")
            
            # Check if URL is allowed before processing
            if not self.is_url_allowed(url):
                logger.warning(f"URL not allowed: {url}")
                return {'success': False}
            
            # Make sure config is available
            if not self.config:
                logger.error("No configuration available for crawler")
                return {'success': False}
            
            # Fetch the page
            logger.info(f"Fetching page: {url}")
            response = requests.get(
                url,
                headers={'User-Agent': self.config.get('crawler', {}).get('user_agent', 'Mozilla/5.0')},
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"Successfully fetched page: {url}")
            
            # Parse the page
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract data using configured rules
            data = {}
            for field, rule in self.config.get('extraction_rules', {}).items():
                try:
                    if not rule or rule.get('selector') is None:
                        continue
                    element = soup.select_one(rule.get('selector'))
                    if element:
                        if rule.get('type') == 'text':
                            data[field] = element.get_text().strip()
                        elif rule.get('type') == 'href':
                            data[field] = element.get('href', '')
                        logger.info(f"Extracted {field}: {data[field]}")
                    else:
                        logger.warning(f"No element found for selector: {rule.get('selector')}")
                except Exception as e:
                    logger.error(f"Error extracting {field}: {str(e)}")
            
            # Add metadata
            data['url'] = url
            data['depth'] = depth
            
            # Find and queue new links
            links = []
            
            # Handle book links
            for book in soup.select('.product_pod'):
                book_link = book.select_one('h3 a')
                if book_link and book_link.get('href'):
                    href = book_link.get('href', '')
                    if href:
                        # Fix typing issue with urljoin by ensuring base_url is a string
                        if isinstance(url, str):
                            base_url = url
                            absolute_url = urljoin(base_url, href)
                            if self.is_url_allowed(absolute_url):
                                links.append({
                                    'url': absolute_url,
                                    'depth': depth + 1
                                })
                                logger.info(f"Found book link: {absolute_url}")
            
            # Handle pagination links
            pagination = soup.select('.pager .next a')
            for page_link in pagination:
                href = page_link.get('href', '')
                if href:
                    # Fix typing issue with urljoin by ensuring base_url is a string
                    if isinstance(url, str):
                        base_url = url
                        absolute_url = urljoin(base_url, href)
                        if self.is_url_allowed(absolute_url):
                            links.append({
                                'url': absolute_url,
                                'depth': depth
                            })
                            logger.info(f"Found pagination link: {absolute_url}")
            
            # Handle category links
            category_links = soup.select('.side_categories .nav-list a')
            for cat_link in category_links:
                href = cat_link.get('href', '')
                if href:
                    # Fix typing issue with urljoin by ensuring base_url is a string
                    if isinstance(url, str):
                        base_url = url
                        absolute_url = urljoin(base_url, href)
                        if self.is_url_allowed(absolute_url):
                            links.append({
                                'url': absolute_url,
                                'depth': depth
                            })
                            logger.info(f"Found category link: {absolute_url}")
            
            logger.info(f"Found {len(links)} links on page: {url}")
            
            return {
                'success': True,
                'data': data,
                'links': links
            }
        
        except Exception as e:
            logger.error(f"Error processing URL {url}: {str(e)}")
            return {'success': False}
    
    def is_url_allowed(self, url):
        """Check if URL is allowed based on domain restrictions"""
        if not self.config:
            return False
            
        parsed_url = urlparse(url)
        allowed_domains = self.config.get('crawler', {}).get('allowed_domains', [])
        is_allowed = any(domain in parsed_url.netloc for domain in allowed_domains)
        return is_allowed
    
    def send_heartbeats(self):
        """Periodically send heartbeat messages to the server"""
        while self.running and self.connected:
            try:
                # Wait a bit before sending the first heartbeat
                time.sleep(30)
                
                if not self.connected or not self.running:
                    break
                
                # Send heartbeat
                heartbeat = {'type': 'heartbeat'}
                if not self.send_data(heartbeat):
                    logger.error("Failed to send heartbeat")
                    self.connected = False
                    break
                
                # Wait for acknowledgment
                response = self.receive_data()
                if not response or response.get('type') != 'heartbeat_ack':
                    logger.error("Failed to receive heartbeat acknowledgment")
                    self.connected = False
                    break
                
                logger.debug("Heartbeat acknowledged")
            
            except Exception as e:
                logger.error(f"Error in heartbeat thread: {str(e)}")
                self.connected = False
                break
    
    def send_data(self, data):
        """Encode and send JSON data to the server"""
        if not self.connected or not self.socket:
            return False
        
        try:
            # Convert data to JSON and encode
            json_data = json.dumps(data).encode('utf-8')
            
            # Send message length (4 bytes) followed by the data
            message_length = len(json_data).to_bytes(4, byteorder='big')
            self.socket.sendall(message_length + json_data)
            return True
        except Exception as e:
            logger.error(f"Error sending data: {str(e)}")
            self.connected = False
            return False
    
    def receive_data(self):
        """Receive and parse JSON data from the server"""
        if not self.connected or not self.socket:
            return None
        
        try:
            # Read message length (4 bytes)
            length_data = self.socket.recv(4)
            if not length_data:
                logger.error("Connection closed by server (no length data)")
                self.connected = False
                return None
            
            message_length = int.from_bytes(length_data, byteorder='big')
            
            # Read the actual message
            data = b''
            while len(data) < message_length:
                chunk = self.socket.recv(min(4096, message_length - len(data)))
                if not chunk:
                    logger.error("Connection closed by server (incomplete data)")
                    self.connected = False
                    return None
                data += chunk
            
            # Parse JSON data
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Error receiving data: {str(e)}")
            self.connected = False
            return None
    
    def shutdown(self):
        """Shutdown the worker"""
        logger.info("Shutting down worker...")
        self.running = False
        
        # Try to send shutdown message to server
        if self.connected and self.socket:
            try:
                shutdown_msg = {'type': 'shutdown'}
                self.send_data(shutdown_msg)
            except:
                pass
        
        # Close connection
        self.disconnect()
        logger.info("Worker shutdown complete")

def main():
    try:
        # Create a custom worker ID based on hostname and random ID
        hostname = socket.gethostname()
        worker_id = f"worker-{hostname}-{uuid.uuid4().hex[:8]}"
        
        # Create and start worker with hardcoded server details
        worker = RemoteWorker(worker_id=worker_id)
        
        logger.info(f"Starting worker {worker_id} connecting to {SERVER_HOST}:{SERVER_PORT}")
        worker.start()
    except KeyboardInterrupt:
        logger.info("Worker interrupted. Shutting down...")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        if 'worker' in locals():
            worker.shutdown()

if __name__ == "__main__":
    main() 