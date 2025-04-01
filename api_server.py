from flask import Flask, jsonify, request
from prometheus_client import generate_latest
import pymongo
import os
import logging
import requests
from typing import Dict, Any, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Get service URLs from environment variables
SCHEDULER_URL = os.getenv('SCHEDULER_URL', 'http://scheduler:5001')
WORKER_URL = os.getenv('WORKER_URL', 'http://worker:5000')

def get_mongodb_client():
    mongo_uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
    return pymongo.MongoClient(mongo_uri)

def get_service_status(url: str) -> Dict[str, Any]:
    """Get the status of a service by its URL."""
    try:
        response = requests.get(f"{url}/status")
        if response.status_code == 200:
            return response.json()
        return {"status": "error", "message": f"Service returned status code {response.status_code}"}
    except requests.RequestException as e:
        return {"status": "error", "message": str(e)}

@app.route('/metrics')
def metrics():
    """Expose Prometheus metrics."""
    return generate_latest()

@app.route('/stats')
def stats():
    """Get crawling statistics."""
    try:
        client = get_mongodb_client()
        db = client["web_crawler"]
        pages = db["pages"]
        visited = db["visited_urls"]
        queue = db["url_queue"]
        
        stats = {
            'total_pages': pages.count_documents({}),
            'total_visited': visited.count_documents({}),
            'queue_size': queue.count_documents({}),
            'queue_status': {
                'pending': queue.count_documents({'status': 'pending'}),
                'processing': queue.count_documents({'status': 'processing'}),
                'completed': queue.count_documents({'status': 'completed'}),
                'failed': queue.count_documents({'status': 'failed'})
            }
        }
        
        client.close()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/search')
def search():
    """Search crawled pages."""
    try:
        query = request.args.get('q', '')
        client = get_mongodb_client()
        db = client["web_crawler"]
        collection = db["pages"]
        
        results = list(collection.find(
            {'$text': {'$search': query}},
            {'score': {'$meta': 'textScore'}}
        ).sort([('score', {'$meta': 'textScore'})]).limit(10))
        
        # Convert ObjectId to string for JSON serialization
        for result in results:
            result['_id'] = str(result['_id'])
        
        client.close()
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error searching: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy'})

@app.route('/')
def index():
    """Root endpoint showing API status."""
    return jsonify({
        'status': 'ok',
        'message': 'Web Scraper API is running',
        'services': {
            'scheduler': SCHEDULER_URL,
            'worker': WORKER_URL
        }
    })

@app.route('/status')
def status():
    """Get status of all services."""
    scheduler_status = get_service_status(SCHEDULER_URL)
    worker_status = get_service_status(WORKER_URL)
    
    return jsonify({
        'api': {'status': 'ok'},
        'scheduler': scheduler_status,
        'worker': worker_status
    })

@app.route('/start', methods=['POST'])
def start_services():
    """Start all services."""
    try:
        # Start scheduler
        scheduler_response = requests.post(f"{SCHEDULER_URL}/start")
        if scheduler_response.status_code != 200:
            return jsonify({'error': 'Failed to start scheduler'}), 500

        # Start worker
        worker_response = requests.post(f"{WORKER_URL}/start", json={'worker_id': 'worker1'})
        if worker_response.status_code != 200:
            return jsonify({'error': 'Failed to start worker'}), 500

        return jsonify({'message': 'All services started successfully'})
    except Exception as e:
        logger.error(f"Error starting services: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/stop', methods=['POST'])
def stop_services():
    """Stop all services."""
    try:
        # Stop scheduler
        scheduler_response = requests.post(f"{SCHEDULER_URL}/stop")
        if scheduler_response.status_code != 200:
            return jsonify({'error': 'Failed to stop scheduler'}), 500

        # Stop worker
        worker_response = requests.post(f"{WORKER_URL}/stop")
        if worker_response.status_code != 200:
            return jsonify({'error': 'Failed to stop worker'}), 500

        return jsonify({'message': 'All services stopped successfully'})
    except Exception as e:
        logger.error(f"Error stopping services: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/add_job', methods=['POST'])
def add_job():
    """Add a new job to the scheduler."""
    try:
        data = request.get_json()
        if not data or 'url' not in data:
            return jsonify({'error': 'URL is required'}), 400
        
        response = requests.post(f"{SCHEDULER_URL}/add_job", json=data)
        return jsonify(response.json())
    except requests.RequestException as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/jobs', methods=['GET'])
def get_jobs():
    """Get all jobs from the scheduler."""
    try:
        response = requests.get(f"{SCHEDULER_URL}/get_jobs")
        return jsonify(response.json())
    except requests.RequestException as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/combined_stats', methods=['GET'])
def get_combined_stats():
    """Get statistics from both scheduler and worker."""
    try:
        scheduler_stats = requests.get(f"{SCHEDULER_URL}/get_queue_stats").json()
        worker_stats = requests.get(f"{WORKER_URL}/stats").json()
        
        return jsonify({
            'status': 'success',
            'scheduler': scheduler_stats,
            'worker': worker_stats
        })
    except requests.RequestException as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5002))
    app.run(host='0.0.0.0', port=port) 