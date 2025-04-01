from flask import Flask, jsonify, request
import os
import logging
from distributed_crawler import DistributedCrawler
import threading
import time
import traceback
import json

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('worker.log')
    ]
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Add a test log message
logger.debug("Logging system initialized")

app = Flask(__name__)
crawler = None
is_running = False

# Poll for jobs function - moved outside to be accessible from multiple functions
def poll_for_jobs():
    global crawler, is_running
    print("POLL THREAD: Starting to poll for jobs", flush=True)
    logger.info("Poll thread started")
    
    # Log RabbitMQ connection details
    if crawler and crawler.connection:
        logger.info(f"RabbitMQ connection state: {'open' if not crawler.connection.is_closed else 'closed'}")
        logger.info(f"RabbitMQ channel state: {'open' if crawler.channel and not crawler.channel.is_closed else 'closed'}")
    
    counter = 0
    no_messages_count = 0
    while is_running and crawler is not None:
        try:
            # Log less frequently to avoid flooding
            if counter % 10 == 0:
                print("POLL THREAD: Checking for messages...", flush=True)
                
                # Check queue status periodically
                try:
                    queue_info = crawler.channel.queue_declare(queue='url_queue', passive=True)
                    message_count = queue_info.method.message_count
                    logger.info(f"RabbitMQ queue status: {message_count} messages waiting")
                    print(f"POLL THREAD: Queue has {message_count} messages waiting", flush=True)
                    
                    # Also check MongoDB queue status
                    mongo_pending = crawler.queue.count_documents({"status": "pending"})
                    logger.info(f"MongoDB queue status: {mongo_pending} pending URLs")
                    print(f"POLL THREAD: MongoDB has {mongo_pending} pending URLs", flush=True)
                except Exception as queue_error:
                    logger.error(f"Failed to get queue status: {str(queue_error)}")
            
            # Process next URL from queue
            result = crawler.process_next_url()
            
            # Log the result of processing
            if result:
                logger.info("Successfully processed a URL")
                print("POLL THREAD: Successfully processed a URL!", flush=True)
                no_messages_count = 0
            else:
                no_messages_count += 1
                # Only log after several consecutive no-message iterations
                if no_messages_count % 10 == 0:
                    logger.info(f"No messages to process for {no_messages_count} iterations")
                    print(f"POLL THREAD: No messages to process for {no_messages_count} iterations", flush=True)
                    
                    # If we've had a lot of empty iterations, check if URLs are actually in MongoDB but not RabbitMQ
                    if no_messages_count % 30 == 0:
                        logger.info("Checking for inconsistency between MongoDB and RabbitMQ")
                        mongo_pending = crawler.queue.count_documents({"status": "pending"})
                        if mongo_pending > 0:
                            # Force process a URL from MongoDB
                            print(f"POLL THREAD: Found {mongo_pending} pending URLs in MongoDB, will attempt to process one", flush=True)
                            url, depth = crawler.get_next_url()
                            if url:
                                logger.info(f"Directly processing URL from MongoDB: {url}")
                                success = crawler.process_url(url, depth)
                                if success:
                                    # Mark as completed
                                    update_result = crawler.queue.update_one(
                                        {"url": url, "status": "processing"},
                                        {"$set": {
                                            "status": "completed",
                                            "completed_at": time.time()
                                        }}
                                    )
                                    logger.info(f"Successfully processed URL from MongoDB: {url}")
                                    no_messages_count = 0
            
            counter += 1
            time.sleep(1)  # Small delay to prevent CPU overuse
        except Exception as e:
            print(f"POLL THREAD ERROR: {str(e)}", flush=True)
            logger.error(f"Error polling for jobs: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Check if RabbitMQ connection is closed and attempt to reconnect
            if crawler and (crawler.connection is None or crawler.connection.is_closed):
                logger.info("Attempting to reconnect to RabbitMQ...")
                try:
                    # First clean up any existing connection
                    crawler.cleanup()
                    # Then reconnect with same worker_id
                    worker_id = crawler.worker_id
                    crawler = DistributedCrawler(worker_id)
                    logger.info(f"Successfully reconnected with worker_id: {worker_id}")
                except Exception as reconnect_error:
                    logger.error(f"Failed to reconnect: {str(reconnect_error)}")
            
            time.sleep(5)  # Wait longer on error
    
    print("POLL THREAD: Stopped polling for jobs", flush=True)

@app.route('/start', methods=['POST'])
def start_worker():
    global crawler, is_running
    
    try:
        logger.info("Received start request")
        print("START ENDPOINT: Received start request", flush=True)
        data = request.get_json()
        logger.info(f"Request data: {data}")
        worker_id = data.get('worker_id')
        reset_db = data.get('reset_database', False)
        
        if not worker_id:
            logger.error("No worker_id provided in request")
            return jsonify({'error': 'worker_id is required'}), 400
            
        logger.info(f"Starting worker with ID: {worker_id}")
        print(f"START ENDPOINT: Starting worker with ID: {worker_id}", flush=True)
        
        # Stop previous worker if running
        if is_running:
            logger.info("Worker already running, stopping it first")
            is_running = False
            time.sleep(2)  # Give the previous thread time to exit
        
        # Clean up existing crawler instance if it exists
        if crawler is not None:
            logger.info("Cleaning up existing crawler instance")
            try:
                crawler.cleanup()
            except Exception as e:
                logger.error(f"Error cleaning up existing crawler: {str(e)}")
        
        # Always create a new crawler instance
        try:
            logger.info("Initializing new crawler instance")
            crawler = DistributedCrawler(worker_id)
            logger.info(f"Successfully initialized crawler for worker {worker_id}")
        except Exception as e:
            logger.error(f"Failed to initialize crawler: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({'error': f"Failed to initialize crawler: {str(e)}"}), 500
        
        # Reset database if requested
        if reset_db and crawler is not None:
            logger.info("Reset database requested during startup")
            print("START ENDPOINT: Resetting database as requested", flush=True)
            
            # Clear collections
            crawler.visited_urls.delete_many({})
            crawler.pages.delete_many({})
            crawler.queue.delete_many({})
            
            logger.info("Successfully cleared all database collections during startup")
            print("START ENDPOINT: Database reset complete", flush=True)
        
        # Set is_running first so thread knows to continue
        is_running = True
        logger.info("Worker started successfully")
        print("START ENDPOINT: Worker started successfully", flush=True)
        
        # Start the polling thread
        logger.info("Starting polling thread...")
        polling_thread = threading.Thread(target=poll_for_jobs)
        polling_thread.daemon = True
        polling_thread.start()
        logger.info(f"Polling thread started with ID: {polling_thread.ident}")
        print(f"START ENDPOINT: Polling thread started with ID: {polling_thread.ident}", flush=True)
        
        return jsonify({'status': 'started', 'worker_id': worker_id, 'thread_id': polling_thread.ident})
    except Exception as e:
        logger.error(f"Error starting worker: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        print(f"START ENDPOINT ERROR: {str(e)}", flush=True)
        return jsonify({'error': str(e)}), 500

@app.route('/stop', methods=['POST'])
def stop_worker():
    global crawler, is_running
    
    try:
        if is_running:
            is_running = False
            if crawler:
                crawler.cleanup()
            logger.info("Stopped worker")
            return jsonify({'status': 'stopped'})
        else:
            return jsonify({'status': 'already_stopped'})
            
    except Exception as e:
        logger.error(f"Error stopping worker: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/status', methods=['GET'])
def get_status():
    try:
        if crawler is None:
            return jsonify({
                'status': 'not_initialized',
                'is_running': False,
                'worker_id': None
            })
            
        return jsonify({
            'status': 'running' if is_running else 'stopped',
            'is_running': is_running,
            'worker_id': crawler.worker_id
        })
        
    except Exception as e:
        logger.error(f"Error getting status: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    try:
        if crawler is None:
            return jsonify({
                'status': 'stopped',
                'error': 'Crawler is not initialized'
            }), 404
            
        stats = crawler.get_stats()
        if stats is None:
            return jsonify({
                'status': 'error',
                'error': 'Failed to get stats'
            }), 500
            
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/test_crawler', methods=['POST'])
def test_crawler():
    global crawler
    
    try:
        if crawler is None:
            return jsonify({
                'status': 'error',
                'error': 'Crawler is not initialized'
            }), 400
            
        # Get a pending URL from MongoDB
        db = crawler.mongo_client["web_crawler"]
        queue = db["url_queue"]
        
        pending_url = queue.find_one({"status": "pending"})
        
        if not pending_url:
            return jsonify({
                'status': 'error',
                'error': 'No pending URLs found'
            }), 404
            
        url = pending_url['url']
        depth = pending_url.get('depth', 0)
        
        # Process the URL directly
        success = crawler.process_url(url, depth)
        
        if success:
            # Mark as completed
            update_result = queue.update_one(
                {"_id": pending_url['_id']},
                {"$set": {
                    "status": "completed",
                    "completed_at": time.time()
                }}
            )
            
            return jsonify({
                'status': 'success',
                'url': url,
                'depth': depth,
                'update_result': {
                    'matched_count': update_result.matched_count,
                    'modified_count': update_result.modified_count
                }
            })
        else:
            return jsonify({
                'status': 'error',
                'error': f'Failed to process URL: {url}'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in test_crawler endpoint: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/process_url', methods=['POST'])
def process_url_endpoint():
    global crawler
    
    try:
        if crawler is None:
            return jsonify({
                'status': 'error',
                'error': 'Crawler is not initialized'
            }), 400
            
        data = request.get_json()
        url = data.get('url')
        depth = data.get('depth', 0)
        
        if not url:
            return jsonify({
                'status': 'error',
                'error': 'URL is required'
            }), 400
            
        print(f"PROCESS URL ENDPOINT: Processing URL: {url}", flush=True)
        success = crawler.process_url(url, depth)
        
        if success:
            return jsonify({
                'status': 'success',
                'url': url,
                'depth': depth
            })
        else:
            return jsonify({
                'status': 'error',
                'error': f'Failed to process URL: {url}'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in process_url endpoint: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/process_mongodb_pending', methods=['POST'])
def process_mongodb_pending():
    global crawler
    
    try:
        if crawler is None:
            return jsonify({
                'status': 'error',
                'error': 'Crawler is not initialized'
            }), 400
        
        # Get URL directly from MongoDB
        url, depth = crawler.get_next_url()
        
        if not url:
            return jsonify({
                'status': 'error',
                'error': 'No pending URLs found in MongoDB'
            }), 404
        
        logger.info(f"Processing URL from MongoDB: {url}")
        print(f"MONGODB PROCESS: Processing URL: {url}", flush=True)
        
        # Process the URL
        success = crawler.process_url(url, depth)
        
        if success:
            # Mark as completed in MongoDB
            update_result = crawler.queue.update_one(
                {"url": url, "status": "processing"},
                {"$set": {
                    "status": "completed",
                    "completed_at": time.time()
                }}
            )
            
            logger.info(f"Successfully processed URL from MongoDB: {url}")
            print(f"MONGODB PROCESS: Successfully processed: {url}", flush=True)
            
            return jsonify({
                'status': 'success',
                'message': 'Successfully processed URL from MongoDB',
                'url': url,
                'depth': depth
            })
        else:
            # Reset URL status to pending in case of failure
            crawler.queue.update_one(
                {"url": url, "status": "processing"},
                {"$set": {
                    "status": "pending"
                }}
            )
            
            logger.warning(f"Failed to process URL from MongoDB: {url}")
            return jsonify({
                'status': 'error',
                'error': f'Failed to process URL: {url}'
            }), 500
    
    except Exception as e:
        logger.error(f"Error in process_mongodb_pending endpoint: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/reset_database', methods=['POST'])
def reset_database():
    global crawler
    
    try:
        if crawler is None:
            return jsonify({
                'status': 'error',
                'error': 'Crawler is not initialized'
            }), 400
            
        # Get confirmation from request data
        data = request.get_json() or {}
        confirm = data.get('confirm', False)
        
        if not confirm:
            return jsonify({
                'status': 'error',
                'error': 'Confirmation required. Send {"confirm": true} to proceed.'
            }), 400
        
        # Clear collections
        logger.info("Clearing database collections as requested")
        print("RESET: Clearing database collections", flush=True)
        
        # Drop collections
        crawler.visited_urls.delete_many({})
        crawler.pages.delete_many({})
        crawler.queue.delete_many({})
        
        logger.info("Successfully cleared all database collections")
        print("RESET: Database reset complete", flush=True)
        
        return jsonify({
            'status': 'success',
            'message': 'Database has been reset. All URLs and pages have been cleared.'
        })
            
    except Exception as e:
        logger.error(f"Error resetting database: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/reset_all', methods=['POST'])
def reset_all():
    global crawler, is_running
    
    try:
        # Get confirmation from request data
        data = request.get_json() or {}
        confirm = data.get('confirm', False)
        
        if not confirm:
            return jsonify({
                'status': 'error',
                'error': 'Confirmation required. Send {"confirm": true} to proceed.'
            }), 400
        
        # First, stop the worker if it's running
        was_running = is_running
        if is_running:
            logger.info("Stopping worker before database reset")
            print("RESET ALL: Stopping worker before reset", flush=True)
            is_running = False
            time.sleep(2)  # Give the worker thread time to exit gracefully
        
        # Initialize a crawler if needed
        if crawler is None:
            logger.info("Initializing a temporary crawler instance for database reset")
            temp_crawler = DistributedCrawler("temp_reset_worker")
        else:
            temp_crawler = crawler
            
        # Clear all collections in the database by dropping them
        logger.info("Clearing ALL MongoDB collections")
        print("RESET ALL: Clearing ALL MongoDB collections", flush=True)
        
        # Get all collection names and drop them
        db = temp_crawler.mongo_client["web_crawler"]
        collections = db.list_collection_names()
        
        # Drop each collection completely
        for collection_name in collections:
            logger.info(f"Dropping collection: {collection_name}")
            print(f"RESET ALL: Dropping collection {collection_name}", flush=True)
            db.drop_collection(collection_name)
        
        # Clear message queue in RabbitMQ
        try:
            # Purge the queue
            temp_crawler.channel.queue_purge(queue='url_queue')
            logger.info("Purged RabbitMQ url_queue")
            print("RESET ALL: Purged RabbitMQ url_queue", flush=True)
        except Exception as e:
            logger.error(f"Error purging RabbitMQ queue: {str(e)}")
            print(f"RESET ALL ERROR: Failed to purge RabbitMQ queue: {str(e)}", flush=True)
            
        # Recreate necessary collections with indexes
        logger.info("Re-creating collections with proper indexes")
        print("RESET ALL: Re-creating collections with proper indexes", flush=True)
        
        # Create visited_urls collection with index
        db.create_collection("visited_urls")
        db["visited_urls"].create_index("url", unique=True)
        
        # Create url_queue collection with indexes
        db.create_collection("url_queue")
        db["url_queue"].create_index("url", unique=True)
        db["url_queue"].create_index("status")
        
        # Create pages collection with index
        db.create_collection("pages")
        db["pages"].create_index("url", unique=True)
        
        # Clean up temp crawler if we created one
        if crawler is None:
            temp_crawler.cleanup()
        
        # If worker was running, restart it
        if was_running:
            logger.info("Worker was running, restarting it")
            print("RESET ALL: Restarting worker", flush=True)
            is_running = True
            
            # Start the polling thread
            polling_thread = threading.Thread(target=poll_for_jobs)
            polling_thread.daemon = True
            polling_thread.start()
        
        logger.info("Successfully reset all database collections and RabbitMQ queue")
        print("RESET ALL: Complete - database and queue reset", flush=True)
        
        return jsonify({
            'status': 'success',
            'message': 'All database collections and RabbitMQ queue have been cleared and reset.'
        })
            
    except Exception as e:
        logger.error(f"Error resetting all collections: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) 