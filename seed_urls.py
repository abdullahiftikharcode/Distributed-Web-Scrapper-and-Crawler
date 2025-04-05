import pymongo
import yaml
import time
import os
import logging
import argparse
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config(config_path='config.yaml'):
    """Load the configuration from YAML file"""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return None

def is_url_allowed(url, allowed_domains):
    """Check if URL is allowed based on domain restrictions"""
    parsed_url = urlparse(url)
    is_allowed = any(domain in parsed_url.netloc for domain in allowed_domains)
    logger.info(f"Checking URL {url} against allowed domains {allowed_domains}: {is_allowed}")
    return is_allowed

def seed_urls(url=None, clear_existing=False):
    """Seed initial URLs into the database"""
    try:
        # Load configuration
        config = load_config()
        if not config:
            logger.error("Failed to load configuration")
            return False
        
        # Get start URL from config or argument
        start_url = url or config.get('crawler', {}).get('start_url')
        if not start_url:
            logger.error("No start URL provided or configured")
            return False
        
        # Check if URL is allowed
        allowed_domains = config.get('crawler', {}).get('allowed_domains', [])
        if not is_url_allowed(start_url, allowed_domains):
            logger.error(f"URL not allowed: {start_url}")
            return False
        
        # Connect to MongoDB
        mongo_uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
        client = pymongo.MongoClient(mongo_uri)
        db = client["web_crawler"]
        url_queue = db["url_queue"]
        
        # Create index if it doesn't exist
        url_queue.create_index("url", unique=True)
        
        # Clear existing entries if requested
        if clear_existing:
            logger.info("Clearing existing queue entries...")
            url_queue.delete_many({})
            db["visited_urls"].delete_many({})
            db["pages"].delete_many({})
        
        # Check if URL is already in queue
        existing = url_queue.find_one({"url": start_url})
        if existing:
            logger.info(f"URL already in queue: {start_url}")
            return True
        
        # Add URL to MongoDB queue
        result = url_queue.insert_one({
            "url": start_url,
            "depth": 0,
            "timestamp": time.time(),
            "status": "pending",
            "added_by": "seeder"
        })
        
        logger.info(f"Added URL to queue: {start_url}, ID: {result.inserted_id}")
        
        # Report queue size
        queue_size = url_queue.count_documents({})
        logger.info(f"Current queue size: {queue_size}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error seeding URLs: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Seed URLs for the web crawler')
    parser.add_argument('--url', help='The URL to seed (overrides config)')
    parser.add_argument('--clear', action='store_true', help='Clear existing queue entries')
    args = parser.parse_args()
    
    if seed_urls(args.url, args.clear):
        logger.info("Successfully seeded URL")
    else:
        logger.error("Failed to seed URL")

if __name__ == "__main__":
    main() 