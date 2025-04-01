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
from pymongo.mongo_client import MongoClient
import threading
from typing import Optional, Dict, Any, List, TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler import URLScheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global variables
config: Optional[Dict[str, Any]] = None
url_queue: Optional[Collection] = None
connection: Optional[BlockingConnection] = None
channel: Optional[BlockingChannel] = None
is_running: bool = False
scheduler_thread: Optional[threading.Thread] = None
mongo_client: Optional[MongoClient] = None
scheduler: Optional['URLScheduler'] = None

def load_config():
    global config
    try:
        # Try local path first
        config_path = 'config.yaml'
        if not os.path.exists(config_path):
            # Try Docker container path
            config_path = '/app/config.yaml'
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded configuration from {config_path}")
        else:
            raise FileNotFoundError(f"Config file not found at {config_path}")
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        config = {
            'crawler': {
                'allowed_domains': ['books.toscrape.com'],
                'start_url': 'http://books.toscrape.com/',
                'max_pages_per_domain': 100,
                'crawl_delay': 2,
                'max_depth': 3,
                'max_retries': 3,
                'timeout': 30,
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        }
        logger.info("Using default configuration")

def connect_to_rabbitmq(max_retries=5, retry_delay=5):
    global connection, channel
    retries = 0
    while retries < max_retries:
        try:
            logger.info("Attempting to connect to RabbitMQ...")
            host = os.getenv('RABBITMQ_HOST', 'rabbitmq')  # Changed default to 'rabbitmq'
            port = int(os.getenv('RABBITMQ_PORT', 5672))
            user = os.getenv('RABBITMQ_USER', 'admin')
            password = os.getenv('RABBITMQ_PASS', 'admin')
            
            logger.info(f"RabbitMQ Connection Details:")
            logger.info(f"Host: {host}")
            logger.info(f"Port: {port}")
            logger.info(f"User: {user}")
            
            # Add a delay before first connection attempt to allow RabbitMQ to fully start
            if retries == 0:
                logger.info("Waiting for RabbitMQ to be fully ready...")
                time.sleep(15)  # Increased delay to 15 seconds
            
            credentials = pika.PlainCredentials(user, password)
            parameters = pika.ConnectionParameters(
                host=host,
                port=port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
                connection_attempts=3,
                retry_delay=5,
                socket_timeout=5
            )
            
            logger.info("Creating RabbitMQ connection...")
            connection = pika.BlockingConnection(parameters)
            logger.info("RabbitMQ connection created successfully")
            
            logger.info("Creating channel...")
            channel = connection.channel()
            logger.info("Channel created successfully")
            
            logger.info("Declaring queue...")
            channel.queue_declare(queue='url_queue', durable=True)
            logger.info("Queue declared successfully")
            
            return True
            
        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.error(f"RabbitMQ connection error: {str(e)}")
            retries += 1
            if retries < max_retries:
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Could not connect to RabbitMQ.")
                return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to RabbitMQ: {str(e)}")
            return False
    return False

def connect_to_mongodb(max_retries=5, retry_delay=5):
    global url_queue, mongo_client
    retries = 0
    while retries < max_retries:
        try:
            logger.info("Attempting to connect to MongoDB...")
            # Use MongoDB URI from config if available, otherwise use environment variable or default
            mongo_uri = config.get('mongodb', {}).get('uri') if config else None
            if not mongo_uri:
                mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
            
            logger.info(f"MongoDB URI: {mongo_uri}")
            
            mongo_client = pymongo.MongoClient(mongo_uri)
            db = mongo_client.web_crawler
            url_queue = db.url_queue
            logger.info("Successfully connected to MongoDB")
            return True
        except Exception as e:
            retries += 1
            logger.warning(f"Failed to connect to MongoDB (attempt {retries}/{max_retries}): {e}")
            if retries < max_retries:
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to MongoDB after maximum retries")
                return False

def initialize_services():
    """Initialize connections to MongoDB and RabbitMQ"""
    global scheduler, config, mongo_client, connection, channel
    logger.info("Starting service initialization...")
    
    try:
        # Load configuration first
        load_config()
        if not config:
            logger.error("Failed to load configuration")
            return False
        
        # Add initial delay to allow services to be fully ready
        logger.info("Waiting for services to be fully ready...")
        time.sleep(5)
        
        logger.info("Attempting to connect to MongoDB...")
        if not connect_to_mongodb():
            logger.error("Failed to connect to MongoDB")
            return False
        
        logger.info("Attempting to connect to RabbitMQ...")
        if not connect_to_rabbitmq():
            logger.error("Failed to connect to RabbitMQ")
            return False
        
        # Initialize the scheduler
        logger.info("Initializing scheduler...")
        try:
            from scheduler import URLScheduler
            scheduler = URLScheduler(
                mongo_client=mongo_client,
                connection=connection,
                channel=channel,
                config=config  # Pass the loaded configuration
            )
            scheduler.running = False  # Ensure it starts in stopped state
            logger.info("Scheduler initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Error initializing scheduler: {str(e)}", exc_info=True)
            cleanup()
            return False
        
    except Exception as e:
        logger.error(f"Service initialization failed: {str(e)}", exc_info=True)
        # Cleanup any partial initialization
        cleanup()
        return False

def is_url_allowed(url):
    """Check if URL is allowed based on domain restrictions"""
    try:
        if not config or 'allowed_domains' not in config:
            logger.error("Configuration not loaded or missing allowed_domains")
            return False
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        return domain in config['allowed_domains']
    except Exception as e:
        logger.error(f"Error checking URL: {e}")
        return False

def seed_urls():
    """Seed initial URLs into MongoDB queue and publish to RabbitMQ"""
    try:
        if url_queue is None or channel is None:
            logger.error("MongoDB or RabbitMQ not initialized")
            return False

        initial_urls = [
            "https://example.com/page1",
            "https://example.com/page2"
        ]
        
        for url in initial_urls:
            if is_url_allowed(url):
                url_queue.insert_one({
                    "url": url,
                    "depth": 0,
                    "status": "pending",
                    "retries": 0,
                    "timestamp": time.time()
                })
                channel.basic_publish(
                    exchange='',
                    routing_key='url_queue',
                    body=url,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                        content_type='text/plain'
                    )
                )
                logger.info(f"Seeded URL: {url}")
        return True
    except Exception as e:
        logger.error(f"Error seeding URLs: {e}")
        return False

def cleanup():
    """Cleanup resources"""
    global connection, channel
    try:
        if channel:
            channel.close()
        if connection:
            connection.close()
        logger.info("Cleanup completed")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

def create_app():
    """Create and configure the Flask application."""
    app = Flask(__name__)
    
    # Initialize services at startup only if not already initialized
    global scheduler
    if scheduler is None:
        try:
            if not initialize_services():
                logger.error("Failed to initialize services")
                scheduler = None
            elif scheduler is None:
                logger.error("Scheduler not initialized after service initialization")
                scheduler = None
        except Exception as e:
            logger.error(f"Failed to initialize services: {str(e)}")
            scheduler = None
    
    @app.route('/')
    def index():
        """Root endpoint."""
        return jsonify({
            'status': 'ok' if scheduler is not None else 'error',
            'message': 'Scheduler service is running' if scheduler is not None else 'Scheduler service failed to initialize',
            'initialized': scheduler is not None
        })
    
    @app.route('/test-connection')
    def test_connection():
        """Test connection to MongoDB and RabbitMQ."""
        try:
            # Test MongoDB connection
            if mongo_client:
                mongo_client.admin.command('ping')
                mongo_status = "Connected"
            else:
                mongo_status = "Not connected"
        except Exception as e:
            mongo_status = f"Error: {str(e)}"
        
        try:
            # Test RabbitMQ connection
            if connection and not connection.is_closed:
                rabbitmq_status = "Connected"
            else:
                rabbitmq_status = "Not connected"
        except Exception as e:
            rabbitmq_status = f"Error: {str(e)}"
        
        return jsonify({
            'mongodb': mongo_status,
            'rabbitmq': rabbitmq_status,
            'rabbitmq_host': os.getenv('RABBITMQ_HOST', 'rabbitmq'),
            'rabbitmq_port': os.getenv('RABBITMQ_PORT', '5672')
        })
    
    @app.route('/start', methods=['POST'])
    def start_scheduler():
        """Start the scheduler."""
        try:
            if scheduler is None:
                # Try to initialize services again if not initialized
                if not initialize_services():
                    return jsonify({'error': 'Failed to initialize scheduler'}), 500
                if scheduler is None:
                    return jsonify({'error': 'Scheduler not initialized'}), 500
            
            scheduler.start()
            return jsonify({'status': 'success', 'message': 'Scheduler started'})
        except Exception as e:
            logger.error(f"Error starting scheduler: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/stop', methods=['POST'])
    def stop_scheduler():
        """Stop the scheduler."""
        try:
            if not scheduler:
                return jsonify({'error': 'Scheduler not initialized'}), 500
            
            scheduler.stop()
            return jsonify({'status': 'success', 'message': 'Scheduler stopped'})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/status', methods=['GET'])
    def get_status():
        """Get scheduler status."""
        try:
            if not scheduler:
                error_msg = 'Scheduler not initialized'
                logger.error(error_msg)
                return jsonify({
                    'error': error_msg,
                    'status': 'error',
                    'details': 'The scheduler component failed to initialize properly.'
                }), 500
            
            response = {
                'status': 'running' if scheduler.running else 'stopped',
                'is_running': scheduler.running,
                'connections': {
                    'mongodb': 'unknown',
                    'rabbitmq': 'unknown'
                },
                'jobs': []
            }
            
            # Check MongoDB connection status
            try:
                if mongo_client:
                    mongo_client.admin.command('ping')
                    response['connections']['mongodb'] = 'connected'
                else:
                    error_msg = "MongoDB client is not initialized"
                    logger.error(error_msg)
                    response['connections']['mongodb'] = 'disconnected'
            except Exception as e:
                error_msg = f"MongoDB connection error: {str(e)}"
                logger.error(error_msg, exc_info=True)
                response['connections']['mongodb'] = f"error: {str(e)}"
            
            # Check RabbitMQ connection status
            try:
                if connection and channel:
                    if channel.is_open and not connection.is_closed:
                        # Try to declare queue passively to verify connection
                        try:
                            channel.queue_declare(queue='url_queue', passive=True)
                            response['connections']['rabbitmq'] = 'connected'
                        except Exception as e:
                            error_msg = f"RabbitMQ queue check failed: {str(e)}"
                            logger.error(error_msg, exc_info=True)
                            response['connections']['rabbitmq'] = f"error: {str(e)}"
                    else:
                        error_msg = "RabbitMQ channel is closed"
                        logger.error(error_msg)
                        response['connections']['rabbitmq'] = 'disconnected'
                else:
                    error_msg = "RabbitMQ connection or channel is not initialized"
                    logger.error(error_msg)
                    response['connections']['rabbitmq'] = 'disconnected'
            except Exception as e:
                error_msg = f"RabbitMQ connection error: {str(e)}"
                logger.error(error_msg, exc_info=True)
                response['connections']['rabbitmq'] = f"error: {str(e)}"
            
            # Only try to get jobs if everything is connected and running
            if (scheduler.running and 
                response['connections']['mongodb'] == 'connected' and 
                response['connections']['rabbitmq'] == 'connected'):
                try:
                    response['jobs'] = scheduler.get_jobs()
                except Exception as e:
                    error_msg = f"Error getting jobs: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    response['jobs'] = []
            
            # If both connections have errors, return 500
            if ('error' in response['connections']['mongodb'] and 
                'error' in response['connections']['rabbitmq']):
                response['status'] = 'error'
                return jsonify(response), 500
            
            return jsonify(response)
            
        except Exception as e:
            error_msg = f"Error in status endpoint: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return jsonify({
                'error': str(e),
                'status': 'error',
                'details': 'An unexpected error occurred while getting scheduler status.'
            }), 500
    
    @app.route('/seed', methods=['POST'])
    def seed_initial_urls():
        """Seed initial URLs from configuration."""
        try:
            if not scheduler:
                logger.error("Scheduler not initialized")
                return jsonify({'error': 'Scheduler not initialized'}), 500
            
            if not scheduler.running:
                logger.error("Scheduler is not running")
                return jsonify({'error': 'Scheduler must be started before seeding URLs'}), 400
            
            if not config or 'crawler' not in config:
                logger.error("Configuration not loaded or missing crawler settings")
                return jsonify({'error': 'Configuration not properly loaded'}), 500
            
            if 'start_url' not in config['crawler']:
                logger.error("Start URL not found in configuration")
                return jsonify({'error': 'Start URL not found in configuration'}), 500
            
            if scheduler.seed_urls():
                logger.info("Successfully seeded initial URLs")
                return jsonify({
                    'status': 'success',
                    'message': 'Initial URLs seeded successfully',
                    'start_url': config['crawler']['start_url']
                })
            else:
                logger.error("Failed to seed URLs")
                return jsonify({'error': 'Failed to seed URLs'}), 500
        except Exception as e:
            logger.error(f"Error seeding URLs: {str(e)}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    
    @app.route('/add_job', methods=['POST'])
    def add_job():
        """Add a new job to the scheduler."""
        try:
            if scheduler is None:
                logger.error("Scheduler not initialized")
                return jsonify({'error': 'Scheduler not initialized'}), 500
            
            if not scheduler.running:
                logger.error("Scheduler is not running")
                return jsonify({'error': 'Scheduler must be started before adding jobs'}), 400
            
            try:
                data = request.get_json(force=True)  # Force JSON parsing
            except Exception as e:
                logger.error(f"Error parsing JSON data: {str(e)}")
                return jsonify({'error': 'Invalid JSON data'}), 400
            
            if not data or 'url' not in data:
                logger.error("URL is required in request data")
                return jsonify({'error': 'URL is required in request data'}), 400
            
            try:
                job_id = scheduler.add_job(data['url'])
                logger.info(f"Successfully added job for URL: {data['url']}")
                return jsonify({
                    'status': 'success',
                    'message': 'Job added successfully',
                    'job_id': job_id,
                    'url': data['url']
                })
            except ValueError as e:
                logger.error(f"Invalid URL: {str(e)}")
                return jsonify({'error': str(e)}), 400
            except Exception as e:
                logger.error(f"Error adding job: {str(e)}")
                return jsonify({'error': str(e)}), 500
        except Exception as e:
            logger.error(f"Error in add_job endpoint: {str(e)}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    
    @app.route('/get_jobs', methods=['GET'])
    def get_jobs():
        """Get all jobs from the scheduler."""
        try:
            if scheduler is None:
                return jsonify({'error': 'Scheduler not initialized'}), 500
            
            jobs = scheduler.get_jobs()
            return jsonify({
                'status': 'success',
                'jobs': jobs
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/get_queue_stats', methods=['GET'])
    def get_queue_stats():
        """Get queue statistics."""
        try:
            if scheduler is None:
                return jsonify({'error': 'Scheduler not initialized'}), 500
            
            stats = scheduler.get_queue_stats()
            return jsonify({
                'status': 'success',
                'stats': stats
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/get_worker_details', methods=['GET'])
    def get_worker_details():
        """Get worker details."""
        try:
            if scheduler is None:
                return jsonify({'error': 'Scheduler not initialized'}), 500
            
            details = scheduler.get_worker_details()
            return jsonify({
                'status': 'success',
                'details': details
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    return app

def main():
    """Run the Flask application."""
    # Note: Services are already initialized by create_app()
    app = create_app()
    try:
        app.run(host='0.0.0.0', port=5001)
    except Exception as e:
        logger.error(f"Error running Flask app: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    main() 