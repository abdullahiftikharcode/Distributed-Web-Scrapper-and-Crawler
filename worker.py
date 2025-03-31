import os
import logging
from distributed_crawler import DistributedCrawler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    try:
        # Get worker ID from environment variable
        worker_id = os.environ.get('WORKER_ID')
        if not worker_id:
            logger.error("WORKER_ID environment variable not set")
            return
            
        logger.info(f"Starting worker {worker_id}")
        
        # Create and run crawler
        crawler = DistributedCrawler(worker_id)
        try:
            logger.info(f"Worker {worker_id} initialized successfully")
            logger.info(f"Worker {worker_id} starting to consume messages...")
            crawler.run()
        except KeyboardInterrupt:
            logger.info(f"Worker {worker_id} shutting down...")
        except Exception as e:
            logger.error(f"Error in worker {worker_id} main loop: {str(e)}", exc_info=True)
        finally:
            logger.info(f"Worker {worker_id} cleaning up...")
            crawler.cleanup()
            
    except Exception as e:
        logger.error(f"Error in worker {worker_id}: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 