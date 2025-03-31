from flask import Flask, jsonify, request
from prometheus_client import generate_latest
import pymongo
import yaml
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def load_config(config_path: str = 'config.yaml') -> dict:
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_mongodb_client():
    config = load_config()
    return pymongo.MongoClient(config['mongodb']['uri'])

@app.route('/metrics')
def metrics():
    """Expose Prometheus metrics."""
    return generate_latest()

@app.route('/stats')
def stats():
    """Get crawling statistics."""
    client = get_mongodb_client()
    db = client[load_config()['mongodb']['database']]
    collection = db[load_config()['mongodb']['collection']]
    
    stats = {
        'total_pages': collection.count_documents({}),
        'unique_domains': len(collection.distinct('domain')),
        'average_depth': collection.aggregate([
            {'$group': {'_id': None, 'avg_depth': {'$avg': '$depth'}}}
        ]).next()['avg_depth'] if collection.count_documents({}) > 0 else 0
    }
    
    client.close()
    return jsonify(stats)

@app.route('/search')
def search():
    """Search crawled pages."""
    query = request.args.get('q', '')
    client = get_mongodb_client()
    db = client[load_config()['mongodb']['database']]
    collection = db[load_config()['mongodb']['collection']]
    
    results = list(collection.find(
        {'$text': {'$search': query}},
        {'score': {'$meta': 'textScore'}}
    ).sort([('score', {'$meta': 'textScore'})]).limit(10))
    
    # Convert ObjectId to string for JSON serialization
    for result in results:
        result['_id'] = str(result['_id'])
    
    client.close()
    return jsonify(results)

@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) 