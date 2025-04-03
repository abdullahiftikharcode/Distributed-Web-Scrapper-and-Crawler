import pymongo
import pika
import json
import time
from datetime import datetime
import yaml
import logging
from urllib.parse import urljoin, urlparse
import os
import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DistributedCrawler:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        logger.info(f"Initializing crawler for worker {worker_id}")
        
        try:
            # Connect to MongoDB
            mongo_uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
            self.mongo_client = pymongo.MongoClient(mongo_uri)
            self.db = self.mongo_client["web_crawler"]
            self.visited_urls = self.db["visited_urls"]
            self.pages = self.db["pages"]
            self.queue = self.db["url_queue"]
            logger.info("Successfully connected to MongoDB")
            
            # Load configuration
            with open('config.yaml', 'r') as f:
                self.config = yaml.safe_load(f)
            logger.info("Successfully loaded configuration")
            
            # Initialize RabbitMQ connection with detailed logging
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
            
            # Verify queue exists and get message count
            queue_info = self.channel.queue_declare(queue='url_queue', passive=True)
            logger.info(f"Verified RabbitMQ queue exists with {queue_info.method.message_count} messages")
            
            # Create indexes for better performance
            self.visited_urls.create_index("url", unique=True)
            self.queue.create_index("url", unique=True)
            logger.info("Created MongoDB indexes")
            
        except Exception as e:
            logger.error(f"Error initializing crawler: {str(e)}", exc_info=True)
            raise
    
    def is_url_allowed(self, url):
        """Check if URL is allowed based on domain restrictions"""
        parsed_url = urlparse(url)
        allowed_domains = self.config['crawler']['allowed_domains']
        is_allowed = any(domain in parsed_url.netloc for domain in allowed_domains)
        logger.info(f"Checking URL {url} against allowed domains {allowed_domains}: {is_allowed}")
        return is_allowed
    
    def normalize_url(self, url):
        """Normalize URL to prevent duplicates with different formats"""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    
    def mark_url_visited(self, url):
        """Mark URL as visited with worker ID and timestamp"""
        normalized_url = self.normalize_url(url)
        try:
            self.visited_urls.insert_one({
                "url": normalized_url,
                "worker_id": self.worker_id,
                "timestamp": time.time()
            })
            logger.info(f"Marked URL as visited: {normalized_url}")
            return True
        except pymongo.errors.DuplicateKeyError:
            logger.debug(f"URL already visited: {normalized_url}")
            return False
        except Exception as e:
            logger.error(f"Error marking URL as visited: {str(e)}")
            return False
    
    def is_url_visited(self, url):
        """Check if URL has been visited by any worker"""
        normalized_url = self.normalize_url(url)
        return self.visited_urls.find_one({"url": normalized_url}) is not None
    
    def add_url_to_queue(self, url, depth):
        """Add URL to queue if it hasn't been visited and is allowed"""
        normalized_url = self.normalize_url(url)
        
        if not self.is_url_allowed(normalized_url):
            logger.debug(f"URL not allowed: {normalized_url}")
            return False
            
        if self.is_url_visited(normalized_url):
            logger.debug(f"URL already visited: {normalized_url}")
            return False
            
        try:
            # Check if URL is already in queue with any status
            existing = self.queue.find_one({"url": normalized_url})
            if existing:
                logger.debug(f"URL already in queue: {normalized_url}")
                return False
                
            # Add URL to queue with pending status
            self.queue.insert_one({
                "url": normalized_url,
                "depth": depth,
                "timestamp": time.time(),
                "status": "pending"
            })
            
            # Publish to RabbitMQ
            message = {
                "url": normalized_url,
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
            
            logger.info(f"Added URL to queue: {normalized_url}")
            return True
        except pymongo.errors.DuplicateKeyError:
            logger.debug(f"URL already in queue: {normalized_url}")
            return False
        except Exception as e:
            logger.error(f"Error adding URL to queue: {str(e)}")
            return False
    
    def get_next_url(self):
        """Get next URL from queue using distributed locking"""
        try:
            # Use findOneAndUpdate for atomic operation
            result = self.queue.find_one_and_update(
                {},
                {"$set": {"worker_id": self.worker_id, "processing_started": time.time()}},
                sort=[("timestamp", 1)],
                return_document=pymongo.ReturnDocument.AFTER
            )
            
            if result:
                logger.info(f"Got next URL: {result['url']}")
                return result["url"], result["depth"]
            logger.debug("No URLs in queue")
            return None, None
        except Exception as e:
            logger.error(f"Error getting next URL: {str(e)}")
            return None, None
    
    def process_url(self, url, depth):
        """Process a single URL and extract data"""
        try:
            logger.info(f"Processing URL: {url}")
            
            # Check if URL is allowed before processing
            if not self.is_url_allowed(url):
                logger.warning(f"URL not allowed: {url}")
                return False
            
            # Fetch the page
            logger.info(f"Fetching page: {url}")
            response = requests.get(
                url,
                headers={'User-Agent': self.config['crawler']['user_agent']},
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"Successfully fetched page: {url}")
            
            # Parse the page
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract data using configured rules
            data = {}
            for field, rule in self.config['extraction_rules'].items():
                try:
                    if rule['selector'] is None:
                        continue
                    element = soup.select_one(rule['selector'])
                    if element:
                        if rule['type'] == 'text':
                            data[field] = element.get_text().strip()
                        elif rule['type'] == 'href':
                            data[field] = element.get('href', '')
                        logger.info(f"Extracted {field}: {data[field]}")
                    else:
                        logger.warning(f"No element found for selector: {rule['selector']}")
                except Exception as e:
                    logger.error(f"Error extracting {field}: {str(e)}")
            
            # Add metadata
            data['url'] = url
            data['depth'] = depth
            data['timestamp'] = time.time()
            data['worker_id'] = self.worker_id
            
            # Store in MongoDB
            self.pages.insert_one(data)
            logger.info(f"Stored data for URL: {url}")
            
            # Find and queue new links
            links_found = 0
            
            # Handle book links
            for book in soup.select('.product_pod'):
                book_link = book.select_one('h3 a')
                if book_link and book_link.get('href'):
                    href = book_link['href']
                    absolute_url = urljoin(url, href)
                    if self.is_url_allowed(absolute_url):
                        if self.add_url_to_queue(absolute_url, depth + 1):
                            links_found += 1
                            logger.info(f"Queued book link: {absolute_url}")
            
            # Handle pagination links
            pagination = soup.select('.pager .next a')
            for page_link in pagination:
                if page_link.get('href'):
                    href = page_link['href']
                    absolute_url = urljoin(url, href)
                    if self.is_url_allowed(absolute_url):
                        if self.add_url_to_queue(absolute_url, depth):
                            links_found += 1
                            logger.info(f"Queued pagination link: {absolute_url}")
            
            # Handle category links
            category_links = soup.select('.side_categories .nav-list a')
            for cat_link in category_links:
                if cat_link.get('href'):
                    href = cat_link['href']
                    absolute_url = urljoin(url, href)
                    if self.is_url_allowed(absolute_url):
                        if self.add_url_to_queue(absolute_url, depth):
                            links_found += 1
                            logger.info(f"Queued category link: {absolute_url}")
            
            logger.info(f"Found and queued {links_found} new links from {url}")
            
            # Mark URL as visited
            self.mark_url_visited(url)
            
            return True
        except Exception as e:
            logger.error(f"Error processing URL {url}: {str(e)}", exc_info=True)
            return False
    
    def run(self):
        """Main crawler loop"""
        logger.info(f"Worker {self.worker_id} starting...")
        
        # Set up RabbitMQ consumer with detailed logging
        logger.info("Setting up RabbitMQ consumer...")
        self.channel.basic_qos(prefetch_count=1)
        logger.info("Set prefetch count to 1")
        
        logger.info("Setting up message callback...")
        self.channel.basic_consume(
            queue='url_queue',
            on_message_callback=self.process_message
        )
        logger.info("Successfully set up message callback")
        
        try:
            logger.info(f"Worker {self.worker_id} starting to consume messages...")
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info(f"Worker {self.worker_id} stopping...")
        except Exception as e:
            logger.error(f"Error in consumer loop: {str(e)}", exc_info=True)
        finally:
            self.cleanup()
    
    def process_message(self, ch, method, properties, body):
        """Process a message from RabbitMQ"""
        try:
            message = json.loads(body)
            url = message['url']
            depth = message.get('depth', 0)
            
            logger.info(f"Processing message for URL: {url}")
            
            # Check if URL is already being processed or completed
            existing = self.queue.find_one({
                "url": url,
                "$or": [
                    {"status": "processing"},
                    {"status": "completed"}
                ]
            })
            if existing:
                logger.info(f"URL already processed or being processed: {url}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Mark URL as processing
            update_result = self.queue.update_one(
                {"url": url, "status": "pending"},
                {"$set": {
                    "status": "processing",
                    "processing_started": time.time(),
                    "worker_id": self.worker_id
                }}
            )
            
            if update_result.modified_count == 0:
                logger.warning(f"Failed to mark URL as processing: {url}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Process URL
            if self.process_url(url, depth):
                logger.info(f"Successfully processed {url}")
                # Mark URL as completed
                self.queue.update_one(
                    {"url": url},
                    {"$set": {
                        "status": "completed",
                        "completed_at": time.time()
                    }}
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logger.error(f"Failed to process URL: {url}")
                # Mark URL as failed
                self.queue.update_one(
                    {"url": url},
                    {"$set": {
                        "status": "failed",
                        "failed_at": time.time()
                    }}
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            self.connection.close()
            self.mongo_client.close()
            logger.info("Cleaned up resources")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

if __name__ == "__main__":
    # Get worker ID from environment variable
    worker_id = os.environ.get('WORKER_ID', '0')
    
    # Create and run crawler
    crawler = DistributedCrawler(worker_id)
    try:
        crawler.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        crawler.cleanup() 