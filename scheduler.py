from flask import Flask, jsonify, request
import os
import logging
import yaml
import time
import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError
from pika.channel import Channel
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
import json
from urllib.parse import urlparse
import pymongo
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
import threading
from typing import Optional, Dict, Any, List, Union, TYPE_CHECKING, cast

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class URLScheduler:
    def __init__(self, mongo_client: Optional[MongoClient] = None, 
                 connection: Optional[BlockingConnection] = None, 
                 channel: Optional[BlockingChannel] = None, 
                 config: Optional[Dict[str, Any]] = None):
        logger.info("Initializing URL Scheduler")
        try:
            # Initialize connection attributes with proper typing
            self.mongo_client: Optional[MongoClient] = None
            self.db: Optional[Database] = None
            self.url_queue: Optional[Collection] = None
            self.connection: Optional[BlockingConnection] = None
            self.channel: Optional[BlockingChannel] = None
            self.running: bool = False
            self.jobs: Dict[str, Any] = {}
            
            # Use provided configuration or load from file
            if config is not None:
                self.config = config
                logger.info("Using provided configuration")
            else:
                # Load configuration with error handling
                try:
                    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
                    if not os.path.exists(config_path):
                        config_path = '/app/config.yaml'  # Try Docker container path
                    
                    if os.path.exists(config_path):
                        with open(config_path, 'r') as f:
                            self.config = yaml.safe_load(f)
                        logger.info(f"Loaded configuration from {config_path}")
                    else:
                        logger.warning("Config file not found, using default configuration")
                        self.config = {
                            'crawler': {
                                'allowed_domains': ['example.com'],
                                'start_url': 'https://example.com'
                            }
                        }
                except Exception as e:
                    logger.error(f"Error loading config: {str(e)}")
                    self.config = {
                        'crawler': {
                            'allowed_domains': ['example.com'],
                            'start_url': 'https://example.com'
                        }
                    }
            
            # Connect to MongoDB
            self._connect_mongodb(mongo_client)
            
            # Connect to RabbitMQ
            self._connect_rabbitmq(connection, channel)
            
        except Exception as e:
            logger.error(f"Error initializing scheduler: {str(e)}", exc_info=True)
            raise

    def _connect_mongodb(self, mongo_client: Optional[MongoClient] = None) -> None:
        """Establish MongoDB connection with health check."""
        try:
            if mongo_client is not None:
                self.mongo_client = mongo_client
            else:
                mongo_uri = os.environ.get("MONGODB_URI", "mongodb://mongodb:27017/")
                self.mongo_client = pymongo.MongoClient(mongo_uri)
            
            # Test the connection
            if self.mongo_client is not None:
                self.mongo_client.admin.command('ping')
                self.db = self.mongo_client["web_crawler"]
                self.url_queue = self.db["url_queue"]
                logger.info("Successfully connected to MongoDB")
            else:
                raise Exception("Failed to establish MongoDB connection")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    def _connect_rabbitmq(self, connection: Optional[BlockingConnection] = None, 
                         channel: Optional[BlockingChannel] = None) -> None:
        """Establish RabbitMQ connection with health check."""
        try:
            if connection is not None and channel is not None:
                self.connection = connection
                self.channel = channel
            else:
                rabbit_host = os.environ.get("RABBITMQ_HOST", "rabbitmq")
                rabbit_port = int(os.environ.get("RABBITMQ_PORT", "5672"))
                rabbit_user = os.environ.get("RABBITMQ_USER", "admin")
                rabbit_pass = os.environ.get("RABBITMQ_PASS", "admin")
                
                credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
                parameters = pika.ConnectionParameters(
                    host=rabbit_host,
                    port=rabbit_port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
                
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
            
            # Test the connection
            if self.channel is not None:
                self.channel.queue_declare(queue='url_queue', durable=True)
                logger.info("Successfully connected to RabbitMQ")
            else:
                raise Exception("Failed to establish RabbitMQ channel")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            raise

    def _ensure_connections(self) -> None:
        """Ensure both MongoDB and RabbitMQ connections are healthy."""
        try:
            # Check MongoDB connection
            if self.mongo_client is not None:
                self.mongo_client.admin.command('ping')
                if self.db is None:
                    self.db = self.mongo_client["web_crawler"]
                if self.url_queue is None:
                    self.url_queue = self.db["url_queue"]
            else:
                raise Exception("MongoDB connection is not initialized")
        except Exception as e:
            logger.error(f"MongoDB connection lost: {str(e)}")
            self._connect_mongodb()

        try:
            # Check RabbitMQ connection
            if self.connection is None or self.connection.is_closed:
                logger.error("RabbitMQ connection lost")
                self._connect_rabbitmq()
            elif self.channel is None or self.channel.is_closed:
                logger.error("RabbitMQ channel is closed")
                if self.connection and not self.connection.is_closed:
                    self.channel = self.connection.channel()
                    self.channel.queue_declare(queue='url_queue', durable=True)
                else:
                    self._connect_rabbitmq()
        except Exception as e:
            logger.error(f"RabbitMQ connection lost: {str(e)}")
            self._connect_rabbitmq()

    def start(self) -> bool:
        """Start the scheduler with connection health check."""
        try:
            self._ensure_connections()
            if not self.running:
                self.running = True
                logger.info("Scheduler started")
                return True
            return False
        except Exception as e:
            logger.error(f"Error starting scheduler: {str(e)}")
            return False
    
    def stop(self) -> bool:
        """Stop the scheduler with connection health check."""
        try:
            self._ensure_connections()
            if self.running:
                self.running = False
                logger.info("Scheduler stopped")
                return True
            return False
        except Exception as e:
            logger.error(f"Error stopping scheduler: {str(e)}")
            return False
    
    def get_jobs(self) -> List[Dict[str, Any]]:
        """Get all jobs with connection health check."""
        try:
            self._ensure_connections()
            jobs = []
            if self.url_queue is not None:
                cursor = self.url_queue.find({})
                for doc in cursor:
                    jobs.append({
                        'url': doc['url'],
                        'status': doc['status'],
                        'timestamp': doc.get('timestamp', None),
                        'depth': doc.get('depth', 0)
                    })
            return jobs
        except Exception as e:
            logger.error(f"Error getting jobs: {str(e)}")
            return []
    
    def add_job(self, url: str) -> Optional[str]:
        """Add a new job with connection health check."""
        try:
            self._ensure_connections()
            
            if not self.is_url_allowed(url):
                raise ValueError(f"URL not allowed: {url}")
            
            if self.url_queue is not None and self.channel is not None:
                # Check if URL is already in queue
                existing = self.url_queue.find_one({"url": url})
                if existing:
                    return str(existing['_id'])
                
                # Add URL to MongoDB queue
                result = self.url_queue.insert_one({
                    "url": url,
                    "depth": 0,
                    "timestamp": time.time(),
                    "status": "pending"
                })
                
                # Publish to RabbitMQ
                message = {
                    "url": url,
                    "depth": 0,
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
                
                return str(result.inserted_id)
            else:
                raise Exception("MongoDB or RabbitMQ connection not available")
        except Exception as e:
            logger.error(f"Error adding job: {str(e)}")
            raise
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Get queue statistics with connection health check."""
        try:
            self._ensure_connections()
            if self.url_queue is not None:
                # Cast to proper type to help the linter
                url_queue = cast(Collection, self.url_queue)
                return {
                    'pending': url_queue.count_documents({'status': 'pending'}),
                    'processing': url_queue.count_documents({'status': 'processing'}),
                    'completed': url_queue.count_documents({'status': 'completed'}),
                    'failed': url_queue.count_documents({'status': 'failed'})
                }
            return {'pending': 0, 'processing': 0, 'completed': 0, 'failed': 0}
        except Exception as e:
            logger.error(f"Error getting queue stats: {str(e)}")
            return {'pending': 0, 'processing': 0, 'completed': 0, 'failed': 0}

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job with connection health check."""
        try:
            self._ensure_connections()
            if self.url_queue is not None:
                job = self.url_queue.find_one({'_id': job_id})
                if job:
                    return {
                        'url': job['url'],
                        'status': job['status'],
                        'timestamp': job.get('timestamp', None),
                        'depth': job.get('depth', 0)
                    }
            return None
        except Exception as e:
            logger.error(f"Error getting job status: {str(e)}")
            return None

    def get_worker_details(self) -> List[Dict[str, Any]]:
        """Get details of all workers with connection health check."""
        try:
            self._ensure_connections()
            if self.url_queue is not None:
                workers = self.url_queue.find_one({"type": "worker"})
                if workers:
                    return workers.get("workers", [])
            return []
        except Exception as e:
            logger.error(f"Error getting worker details: {str(e)}")
            return []
    
    def is_url_allowed(self, url: str) -> bool:
        """Check if URL is allowed based on domain restrictions"""
        try:
            if not self.config or 'crawler' not in self.config or 'allowed_domains' not in self.config['crawler']:
                logger.error("Configuration not properly loaded or missing allowed_domains")
                return False
                
            parsed_url = urlparse(url)
            allowed_domains = self.config['crawler']['allowed_domains']
            allowed = any(domain in parsed_url.netloc for domain in allowed_domains)
            logger.info(f"Checking URL {url} against allowed domains {allowed_domains}: {'allowed' if allowed else 'not allowed'}")
            return allowed
        except Exception as e:
            logger.error(f"Error checking URL: {str(e)}")
            return False
    
    def seed_urls(self) -> bool:
        """Seed initial URLs with connection health check."""
        try:
            self._ensure_connections()
            if self.url_queue is not None and self.channel is not None:
                start_url = self.config['crawler']['start_url']
                if not self.is_url_allowed(start_url):
                    logger.error(f"Start URL {start_url} not allowed")
                    return False
                
                # Check if URL is already in queue
                existing = self.url_queue.find_one({"url": start_url})
                if existing:
                    logger.info(f"Start URL {start_url} already in queue")
                    return True
                
                # Add URL to MongoDB queue
                result = self.url_queue.insert_one({
                    "url": start_url,
                    "depth": 0,
                    "timestamp": time.time(),
                    "status": "pending"
                })
                
                # Publish to RabbitMQ
                message = {
                    "url": start_url,
                    "depth": 0,
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
                
                logger.info(f"Successfully seeded start URL: {start_url}")
                return True
            else:
                raise Exception("MongoDB or RabbitMQ connection not available")
        except Exception as e:
            logger.error(f"Error seeding URLs: {str(e)}")
            return False
    
    def cleanup(self) -> None:
        """Clean up resources with proper null checks."""
        try:
            if self.channel is not None and not self.channel.is_closed:
                self.channel.close()
            if self.connection is not None and not self.connection.is_closed:
                self.connection.close()
            if self.mongo_client is not None:
                self.mongo_client.close()
            logger.info("Successfully cleaned up resources")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    def is_running(self) -> bool:
        """Check if scheduler is running with connection health check."""
        try:
            self._ensure_connections()
            return self.running
        except Exception as e:
            logger.error(f"Error checking scheduler status: {str(e)}")
            return False

    def verify_queue(self) -> Dict[str, Any]:
        """Verify queue state with connection health check."""
        try:
            self._ensure_connections()
            if self.url_queue is not None and self.channel is not None:
                # Cast to proper types to help the linter
                url_queue = cast(Collection, self.url_queue)
                channel = cast(Channel, self.channel)
                
                # Verify MongoDB queue
                pending_count = url_queue.count_documents({"status": "pending"})
                
                # Verify RabbitMQ queue
                queue_info = channel.queue_declare(queue='url_queue', passive=True)
                rabbitmq_count = queue_info.method.message_count
                
                return {
                    "mongodb_pending": pending_count,
                    "rabbitmq_messages": rabbitmq_count,
                    "status": "healthy" if pending_count == rabbitmq_count else "inconsistent"
                }
            return {
                "mongodb_pending": 0,
                "rabbitmq_messages": 0,
                "status": "unavailable"
            }
        except Exception as e:
            logger.error(f"Error verifying queue: {str(e)}")
            return {
                "mongodb_pending": 0,
                "rabbitmq_messages": 0,
                "status": "error"
            }

def main():
    """Main function that runs only when the script is run directly."""
    try:
        # Only create a new scheduler if running as standalone script
        if __name__ == "__main__":
            scheduler = URLScheduler()
            logger.info("Starting URL scheduler...")
            
            # Seed initial URLs
            logger.info("Attempting to seed initial URLs...")
            if scheduler.seed_urls():
                logger.info("Successfully seeded initial URLs")
                
                # Verify queue state
                queue_size = scheduler.url_queue.count_documents({"status": "pending"})
                logger.info(f"Current queue size: {queue_size}")
                
                # Verify RabbitMQ queue
                queue_info = scheduler.channel.queue_declare(queue='url_queue', passive=True)
                logger.info(f"RabbitMQ queue message count: {queue_info.method.message_count}")
            else:
                logger.error("Failed to seed initial URLs")
                
            # Keep the scheduler running and monitor queue
            logger.info("Starting queue monitoring...")
            try:
                while True:
                    # Check queue status
                    pending_count = scheduler.url_queue.count_documents({"status": "pending"})
                    processing_count = scheduler.url_queue.count_documents({"status": "processing"})
                    completed_count = scheduler.url_queue.count_documents({"status": "completed"})
                    failed_count = scheduler.url_queue.count_documents({"status": "failed"})
                    
                    logger.info(f"Queue status - Pending: {pending_count}, Processing: {processing_count}, "
                              f"Completed: {completed_count}, Failed: {failed_count}")
                    
                    time.sleep(5)  # Check every 5 seconds
                    
            except KeyboardInterrupt:
                logger.info("Scheduler shutting down...")
            finally:
                scheduler.cleanup()
                
    except Exception as e:
        logger.error(f"Error in scheduler main: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 