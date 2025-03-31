import pymongo
import pika
import json
import yaml
import logging
import time
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class URLScheduler:
    def __init__(self):
        logger.info("Initializing URL Scheduler")
        try:
            # Connect to MongoDB
            mongo_uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
            self.mongo_client = pymongo.MongoClient(mongo_uri)
            self.db = self.mongo_client["web_crawler"]
            self.url_queue = self.db["url_queue"]
            logger.info("Connected to MongoDB")
            
            # Load configuration
            with open('config.yaml', 'r') as f:
                self.config = yaml.safe_load(f)
            logger.info("Loaded configuration")
            
            # Connect to RabbitMQ with more detailed logging
            logger.info("Attempting to connect to RabbitMQ...")
            rabbit_host = os.environ.get("RABBITMQ_HOST", "localhost")
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host))
            logger.info("Successfully connected to RabbitMQ")
            
            self.channel = self.connection.channel()
            logger.info("Created RabbitMQ channel")
            
            # Declare queue with durability
            logger.info("Declaring RabbitMQ queue...")
            self.channel.queue_declare(queue='url_queue', durable=True)
            logger.info("Successfully declared RabbitMQ queue")
            
            # Verify queue exists
            queue_info = self.channel.queue_declare(queue='url_queue', passive=True)
            logger.info(f"Verified RabbitMQ queue exists with {queue_info.method.message_count} messages")
            
        except Exception as e:
            logger.error(f"Error initializing scheduler: {str(e)}", exc_info=True)
            raise
    
    def is_url_allowed(self, url):
        """Check if URL is allowed based on domain restrictions"""
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        allowed = any(domain in parsed_url.netloc for domain in self.config['crawler']['allowed_domains'])
        logger.info(f"URL {url} allowed: {allowed}")
        return allowed
    
    def seed_urls(self):
        """Seed initial URLs into the queue"""
        try:
            start_url = self.config['crawler']['start_url']
            logger.info(f"Seeding URL: {start_url}")
            
            if not self.is_url_allowed(start_url):
                logger.error(f"URL not allowed: {start_url}")
                return False
            
            # Check if URL is already in queue
            existing = self.url_queue.find_one({"url": start_url})
            if existing:
                logger.info(f"URL already in queue: {start_url}")
                return True
            
            # Add URL to MongoDB queue
            result = self.url_queue.insert_one({
                "url": start_url,
                "depth": 0,
                "timestamp": time.time(),
                "status": "pending"
            })
            logger.info(f"Added URL to MongoDB queue: {start_url}, ID: {result.inserted_id}")
            
            # Publish to RabbitMQ with persistent message
            message = {
                "url": start_url,
                "depth": 0,
                "timestamp": time.time()
            }
            logger.info("Publishing message to RabbitMQ...")
            self.channel.basic_publish(
                exchange='',
                routing_key='url_queue',
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type='application/json'
                )
            )
            logger.info(f"Successfully published URL to RabbitMQ: {start_url}")
            
            # Verify URL was added to queue
            queue_size = self.url_queue.count_documents({"status": "pending"})
            logger.info(f"Current pending queue size: {queue_size}")
            
            # Verify RabbitMQ queue
            queue_info = self.channel.queue_declare(queue='url_queue', passive=True)
            logger.info(f"RabbitMQ queue message count: {queue_info.method.message_count}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error seeding URLs: {str(e)}", exc_info=True)
            return False
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            self.connection.close()
            self.mongo_client.close()
            logger.info("Cleaned up resources")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

def main():
    try:
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