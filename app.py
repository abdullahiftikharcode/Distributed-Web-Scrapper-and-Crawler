import streamlit as st
import pymongo
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os
import subprocess
import yaml
import time
import pika
import psutil
import traceback
import logging
import threading
import socket
import json
from server import CrawlerServer

# Set page config - must be the first Streamlit command
st.set_page_config(
    page_title="Distributed Web Crawler",
    page_icon="🕷️",
    layout="wide"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize session state
if 'url' not in st.session_state:
    st.session_state.url = "http://books.toscrape.com/"
if 'server_started' not in st.session_state:
    st.session_state.server_started = False
if 'server_thread' not in st.session_state:
    st.session_state.server_thread = None
if 'server_instance' not in st.session_state:
    st.session_state.server_instance = None
if 'server_host' not in st.session_state:
    st.session_state.server_host = '0.0.0.0'
if 'server_port' not in st.session_state:
    st.session_state.server_port = 5555
if 'last_worker_count' not in st.session_state:
    st.session_state.last_worker_count = 0
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()
if 'worker_events' not in st.session_state:
    st.session_state.worker_events = []

def check_rabbitmq():
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        connection.close()
        return True
    except Exception:
        return False

def check_mongodb_installation():
    """Check if MongoDB is installed and running"""
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        client.server_info()
        client.close()
        return True
    except ServerSelectionTimeoutError:
        st.error("MongoDB is not running. Please start MongoDB first.")
        st.error("To start MongoDB, open a new terminal and run: mongod")
        return False
    except ConnectionFailure:
        st.error("Could not connect to MongoDB. Please check if MongoDB is installed and running.")
        st.error("To start MongoDB, open a new terminal and run: mongod")
        return False
    except Exception as e:
        st.error(f"Error checking MongoDB: {str(e)}")
        st.error("To start MongoDB, open a new terminal and run: mongod")

def check_service_status():
    status = {
        'mongodb': check_mongodb_installation(),
        'rabbitmq': check_rabbitmq(),
        'server': st.session_state.server_started
    }
    return status

def show_service_setup_instructions():
    st.error("""
    Some required services are not running. Please follow these steps:

    1. MongoDB Setup:
       - Go to https://www.mongodb.com/try/download/community
       - Download and install MongoDB Community Server
       - Start MongoDB service: `net start MongoDB`

    2. RabbitMQ Setup:
       - Go to https://www.rabbitmq.com/download.html
       - Download and install RabbitMQ Server
       - Start RabbitMQ service: `net start RabbitMQ`

    3. After installing both services, refresh this page.
    """)
    st.stop()

def clean_price(price):
    if pd.isna(price) or price == '':
        return np.nan
    if isinstance(price, (int, float)):
        return float(price)
    if isinstance(price, str):
        clean = ''.join(c for c in price if c.isdigit() or c == '.' or c == ',')
        clean = clean.replace(',', '')
        try:
            return float(clean)
        except (ValueError, TypeError):
            return np.nan
    return np.nan

@st.cache_resource
def get_database():
    if not check_mongodb_installation():
        show_service_setup_instructions()
    
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["web_crawler"]
        return db["pages"]
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {str(e)}")
        return None

@st.cache_data
def load_data():
    collection = get_database()
    if collection is None:
        return pd.DataFrame()
    
    try:
        data = list(collection.find())
        if not data:
            st.info("No data found in the database. Please run the crawler to collect data.")
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df = df.drop('_id', axis=1, errors='ignore')
        
        if 'price' in df.columns:
            df['price'] = df['price'].apply(clean_price)
            df = df.dropna(subset=['price'])
            if df.empty:
                st.warning("No valid price data found in the database.")
                return pd.DataFrame()
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()

def load_config():
    try:
        with open('config.yaml', 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        st.error(f"Error loading config: {str(e)}")
        return None

def save_config(config):
    try:
        with open('config.yaml', 'w') as f:
            yaml.dump(config, f)
        return True
    except Exception as e:
        st.error(f"Error saving config: {str(e)}")
        return False

def start_server_thread():
    """Start the server in a separate thread"""
    try:
        if st.session_state.server_started:
            st.warning("Server is already running")
            return True
        
        # Create server instance
        server = CrawlerServer(
            host=st.session_state.server_host,
            port=st.session_state.server_port
        )
        
        # Store server instance in session state
        st.session_state.server_instance = server
        
        # Create and start server thread
        server_thread = threading.Thread(target=server.start)
        server_thread.daemon = True
        server_thread.start()
        
        # Store thread in session state
        st.session_state.server_thread = server_thread
        st.session_state.server_started = True
        
        logger.info("Server started in background thread")
        st.success(f"Server started on {st.session_state.server_host}:{st.session_state.server_port}")
        return True
    except Exception as e:
        logger.error(f"Error starting server: {str(e)}")
        st.error(f"Failed to start server: {str(e)}")
        return False

def stop_server():
    """Stop the running server"""
    try:
        if not st.session_state.server_started:
            st.warning("Server is not running")
            return True
        
        # Shutdown server
        if st.session_state.server_instance:
            st.session_state.server_instance.shutdown()
        
        # Reset session state
        st.session_state.server_instance = None
        st.session_state.server_thread = None
        st.session_state.server_started = False
        
        logger.info("Server stopped")
        st.success("Server stopped")
        return True
    except Exception as e:
        logger.error(f"Error stopping server: {str(e)}")
        st.error(f"Failed to stop server: {str(e)}")
        return False

def seed_initial_urls():
    """Seed initial URLs to the crawler queue"""
    try:
        if not st.session_state.server_started:
            st.error("Server is not running. Please start the server first.")
            return False
        
        if not st.session_state.server_instance:
            st.error("Server instance not found.")
            return False
        
        # Get configuration
        config = load_config()
        if not config:
            st.error("Failed to load configuration")
            return False
        
        # Clear existing data
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client["web_crawler"]
            db.pages.delete_many({})
            db.visited_urls.delete_many({})
            db.url_queue.delete_many({})
            logger.info("Cleared existing data")
        except Exception as e:
            logger.error(f"Error clearing data: {str(e)}")
            st.error(f"Error clearing data: {str(e)}")
            return False
        
        # Add initial URL to queue
        start_url = config.get('crawler', {}).get('start_url', 'http://books.toscrape.com/')
        result = st.session_state.server_instance.add_url_to_queue(start_url, 0, "scheduler")
        
        if result:
            logger.info(f"Successfully seeded initial URL: {start_url}")
            st.success(f"Successfully seeded initial URL: {start_url}")
            return True
        else:
            logger.error("Failed to seed initial URL")
            st.error("Failed to seed initial URL")
            return False
    except Exception as e:
        logger.error(f"Error seeding initial URLs: {str(e)}")
        st.error(f"Error seeding initial URLs: {str(e)}")
        return False

def get_worker_details():
    """Get worker details from the server"""
    try:
        if not st.session_state.server_started or not st.session_state.server_instance:
            return {}
        
        # Get worker status from server
        return {
            'workers': st.session_state.server_instance.get_worker_status(),
            'mongodb_workers': get_mongodb_worker_stats()
        }
    except Exception as e:
        logger.error(f"Error getting worker details: {str(e)}")
        return {}

def get_mongodb_worker_stats():
    """Get worker statistics from MongoDB"""
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["web_crawler"]
        worker_status = db["worker_status"].find()
        
        # Process and return worker status data
        workers = {}
        for status in worker_status:
            worker_id = status.get('worker_id')
            workers[worker_id] = {
                'status': status.get('status', 'unknown'),
                'last_activity': status.get('last_activity'),
                'address': status.get('address', 'unknown'),
                'connected_at': status.get('connected_at'),
                'current_url': status.get('current_url'),
                'last_completed_url': status.get('last_completed_url')
            }
        
        # Add page counts
        for worker_id in workers:
            workers[worker_id]['pages_crawled'] = db["pages"].count_documents({"worker_id": worker_id})
        
        return workers
    except Exception as e:
        logger.error(f"Error getting MongoDB worker stats: {str(e)}")
        return {}

def get_url_queue_stats():
    """Get URL queue statistics"""
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["web_crawler"]
        
        stats = {
            'pending': db["url_queue"].count_documents({"status": "pending"}),
            'processing': db["url_queue"].count_documents({"status": "processing"}),
            'completed': db["url_queue"].count_documents({"status": "completed"}),
            'failed': db["url_queue"].count_documents({"status": "failed"}),
            'total': db["url_queue"].count_documents({})
        }
        
        return stats
    except Exception as e:
        logger.error(f"Error getting URL queue stats: {str(e)}")
        return {}

def get_crawling_logs():
    """Get recent crawling logs from MongoDB"""
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["web_crawler"]
        
        # Get recent visited URLs
        visited = list(db["visited_urls"].find().sort("timestamp", -1).limit(10))
        
        # Get recent completed pages
        crawled = list(db["pages"].find().sort("timestamp", -1).limit(10))
        
        # Combine and format logs
        logs = []
        for v in visited:
            logs.append({
                'type': 'visited',
                'url': v.get('url'),
                'worker_id': v.get('worker_id'),
                'timestamp': v.get('timestamp')
            })
        
        for c in crawled:
            logs.append({
                'type': 'crawled',
                'url': c.get('url'),
                'worker_id': c.get('worker_id'),
                'timestamp': c.get('timestamp'),
                'title': c.get('title'),
                'price': c.get('price')
            })
        
        # Sort by timestamp
        logs.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
        
        return logs[:20]  # Return most recent 20 logs
    except Exception as e:
        logger.error(f"Error getting crawling logs: {str(e)}")
        return []

def main():
    st.title("🕷️ Distributed Web Crawler Dashboard")
    
    # Add refresh button and auto-refresh feature
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.session_state.last_refresh = datetime.now()
            st.rerun()
    with col2:
        auto_refresh = st.checkbox("Auto-refresh (5s)", value=False)
        if auto_refresh:
            st.empty()
            # Add auto-refresh mechanism that will force a rerun every 5 seconds
            time.sleep(5)
            st.session_state.last_refresh = datetime.now()
            st.rerun()
    with col3:
        st.caption(f"Last updated: {st.session_state.last_refresh.strftime('%H:%M:%S')}")
    
    # Service status check
    status = check_service_status()
    
    st.sidebar.header("Service Status")
    if status['mongodb']:
        st.sidebar.success("MongoDB: Running")
    else:
        st.sidebar.error("MongoDB: Not Running")
    
    if status['rabbitmq']:
        st.sidebar.success("RabbitMQ: Running")
    else:
        st.sidebar.error("RabbitMQ: Not Running")
    
    if status['server']:
        st.sidebar.success("Server: Running")
    else:
        st.sidebar.warning("Server: Not Running")
    
    # Get and display worker details
    worker_details = get_worker_details()
    active_workers = worker_details.get('workers', [])
    mongodb_workers = worker_details.get('mongodb_workers', {})
    
    # Check for worker connection/disconnection events
    current_worker_count = len(active_workers)
    if current_worker_count > st.session_state.last_worker_count:
        # Worker connected
        st.session_state.worker_events.append({
            'time': datetime.now(),
            'type': 'connect',
            'count': current_worker_count
        })
        st.toast(f"New worker connected! Total workers: {current_worker_count}", icon="🔌")
    elif current_worker_count < st.session_state.last_worker_count:
        # Worker disconnected
        st.session_state.worker_events.append({
            'time': datetime.now(),
            'type': 'disconnect',
            'count': current_worker_count
        })
        st.toast(f"Worker disconnected! Remaining workers: {current_worker_count}", icon="🔌")
    
    # Update the last worker count
    st.session_state.last_worker_count = current_worker_count
    
    st.sidebar.markdown("---")
    st.sidebar.header("Worker Details")
    
    # Show active workers
    num_workers = len(active_workers)
    if num_workers > 0:
        st.sidebar.success(f"Active Workers: {num_workers}")
        
        for worker in active_workers:
            worker_id = worker.get('worker_id')
            with st.sidebar.expander(f"Worker {worker_id} (Active)"):
                st.text(f"Status: {worker.get('status', 'Unknown')}")
                st.text(f"Address: {worker.get('address', 'Unknown')}")
                if worker.get('current_task'):
                    st.text(f"Current Task: {worker.get('current_task')}")
                
                # Add MongoDB stats if available
                if worker_id in mongodb_workers:
                    mongo_stats = mongodb_workers[worker_id]
                    st.metric("Pages Crawled", mongo_stats.get('pages_crawled', 0))
                    if mongo_stats.get('last_completed_url'):
                        st.text(f"Last URL: {mongo_stats.get('last_completed_url')}")
    else:
        # Show inactive/historical workers
        if mongodb_workers:
            st.sidebar.warning("No Active Workers")
            st.sidebar.info(f"Historical Workers: {len(mongodb_workers)}")
            
            for worker_id, stats in mongodb_workers.items():
                with st.sidebar.expander(f"Worker {worker_id} ({stats.get('status', 'Unknown')})"):
                    st.text(f"Status: {stats.get('status', 'Unknown')}")
                    st.text(f"Address: {stats.get('address', 'Unknown')}")
                    st.metric("Pages Crawled", stats.get('pages_crawled', 0))
                    
                    if stats.get('last_activity'):
                        last_active = datetime.fromtimestamp(stats.get('last_activity'))
                        st.text(f"Last Active: {last_active.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    if stats.get('last_completed_url'):
                        st.text(f"Last URL: {stats.get('last_completed_url')}")
        else:
            st.sidebar.warning("No Workers Found")
            st.sidebar.info("Workers will appear here once they connect to the server.")
    
    # Queue statistics
    st.sidebar.markdown("---")
    st.sidebar.header("Queue Statistics")
    queue_stats = get_url_queue_stats()
    
    if queue_stats:
        col1, col2 = st.sidebar.columns(2)
        with col1:
            st.metric("Pending", queue_stats.get('pending', 0))
            st.metric("Completed", queue_stats.get('completed', 0))
        with col2:
            st.metric("Processing", queue_stats.get('processing', 0))
            st.metric("Failed", queue_stats.get('failed', 0))
    else:
        st.sidebar.info("No queue statistics available")
    
    # Server Controls
    st.sidebar.markdown("---")
    st.sidebar.header("Server Controls")
    
    # Server host/port configuration
    with st.sidebar.expander("Server Configuration"):
        new_host = st.text_input("Server Host", value=st.session_state.server_host)
        new_port = st.number_input("Server Port", value=st.session_state.server_port, min_value=1024, max_value=65535)
        
        if st.button("Update Server Configuration"):
            st.session_state.server_host = new_host
            st.session_state.server_port = new_port
            st.success("Server configuration updated")
            st.rerun()
    
    # Server start/stop buttons
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if not st.session_state.server_started:
            if st.button("Start Server", type="primary"):
                if status['mongodb'] and status['rabbitmq']:
                    if start_server_thread():
                        st.rerun()
                else:
                    show_service_setup_instructions()
        else:
            st.info(f"Server running on {st.session_state.server_host}:{st.session_state.server_port}")
    
    with col2:
        if st.session_state.server_started:
            if st.button("Stop Server", type="secondary"):
                if stop_server():
                    st.rerun()
    
    # URL Seeding
    st.sidebar.markdown("---")
    if st.session_state.server_started:
        url = st.sidebar.text_input("Start URL", value=st.session_state.url)
        st.session_state.url = url
        
        # Update config with new URL
        config = load_config()
        if config and url != config.get('crawler', {}).get('start_url', ''):
            if 'crawler' not in config:
                config['crawler'] = {}
            config['crawler']['start_url'] = url
            save_config(config)
        
        if st.sidebar.button("Seed URL", type="primary"):
            if seed_initial_urls():
                st.rerun()
    
    # Worker Instructions
    st.sidebar.markdown("---")
    with st.sidebar.expander("Worker Connection Instructions"):
        st.code(f"""
        # To connect a worker from another machine:
        python remote_worker.py --server {st.session_state.server_host} --port {st.session_state.server_port}
        
        # Optionally specify worker ID:
        python remote_worker.py --server {st.session_state.server_host} --port {st.session_state.server_port} --id worker-custom-name
        """)
    
    # Database management
    st.sidebar.markdown("---")
    st.sidebar.header("Database Management")
    
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("Clear Database", type="secondary"):
            try:
                client = pymongo.MongoClient("mongodb://localhost:27017/")
                db = client["web_crawler"]
                
                # Count records before deletion
                pages_count = db["pages"].count_documents({})
                queue_count = db["url_queue"].count_documents({})
                visited_count = db["visited_urls"].count_documents({})
                
                # Delete all records
                db["pages"].delete_many({})
                db["url_queue"].delete_many({})
                db["visited_urls"].delete_many({})
                
                st.sidebar.success(f"Database cleared successfully! Removed {pages_count} pages, {queue_count} queued URLs, and {visited_count} visited URLs.")
                
                # Refresh the data in the dashboard
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"Error clearing database: {str(e)}")
    
    with col2:
        if st.button("Stop Crawling", type="secondary"):
            try:
                client = pymongo.MongoClient("mongodb://localhost:27017/")
                db = client["web_crawler"]
                
                # Count pending URLs before updating
                pending_count = db["url_queue"].count_documents({"status": "pending"})
                
                # Update all pending URLs to cancelled
                result = db["url_queue"].update_many(
                    {"status": "pending"},
                    {"$set": {"status": "cancelled"}}
                )
                
                # Show success message
                modified_count = result.modified_count
                st.sidebar.success(f"Stopped crawling! Cancelled {modified_count} pending URLs.")
                
                # Refresh the data in the dashboard
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"Error stopping crawl: {str(e)}")
    
    st.sidebar.info("These actions can't be undone. Clear Database removes all data. Stop Crawling cancels pending URLs but keeps collected data.")
    
    # Main content area
    tab1, tab2, tab3 = st.tabs(["Dashboard", "Crawled Data", "Activity Log"])
    
    with tab1:
        st.header("System Dashboard")
        
        # System status summary
        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader("Services")
            for service, running in status.items():
                if running:
                    st.success(f"{service.capitalize()}: Running")
                else:
                    st.error(f"{service.capitalize()}: Not Running")
        
        with col2:
            st.subheader("Workers")
            st.metric("Active Workers", len(active_workers))
            st.metric("Total Workers", len(mongodb_workers))
        
        with col3:
            st.subheader("Queue")
            st.metric("Pending URLs", queue_stats.get('pending', 0))
            st.metric("Total URLs", queue_stats.get('total', 0))
        
        # URLs in queue
        col1, col2 = st.columns(2)
        
        with col1:
            st.header("URLs in Queue")
            try:
                client = pymongo.MongoClient("mongodb://localhost:27017/")
                db = client["web_crawler"]
                pending_urls = list(db["url_queue"].find({"status": "pending"}).sort("timestamp", 1).limit(5))
                
                if pending_urls:
                    for url in pending_urls:
                        st.info(f"URL: {url.get('url')} (Depth: {url.get('depth')})")
                else:
                    st.info("No pending URLs in queue")
            except Exception as e:
                st.error(f"Error getting pending URLs: {str(e)}")
        
        with col2:
            st.header("Worker Connection Events")
            if st.session_state.worker_events:
                # Display the most recent events first (up to 10)
                for event in list(reversed(st.session_state.worker_events))[:10]:
                    event_time = event['time'].strftime('%H:%M:%S')
                    if event['type'] == 'connect':
                        st.success(f"{event_time} - Worker connected (Total: {event['count']})")
                    else:
                        st.warning(f"{event_time} - Worker disconnected (Remaining: {event['count']})")
            else:
                st.info("No worker connection events recorded yet.")
                
            # Add a button to clear the event log
            if st.button("Clear Event Log"):
                st.session_state.worker_events = []
                st.rerun()
    
    with tab2:
        st.header("Crawled Data")
        df = load_data()
        
        if df.empty:
            st.info("No data available. Please run the crawler to collect data.")
        else:
            # Filter controls
            st.subheader("Filters")
            if 'price' in df.columns and len(df) > 0:
                min_price = float(df['price'].min())
                max_price = float(df['price'].max())
                
                if min_price == max_price:
                    st.text(f"Fixed Price: ${min_price:.2f}")
                else:
                    price_range = st.slider(
                        "Price Range ($)",
                        min_value=min_price,
                        max_value=max_price,
                        value=(min_price, max_price),
                        step=0.01
                    )
                    df = df[(df['price'] >= price_range[0]) & (df['price'] <= price_range[1])]
            
            if 'category' in df.columns and not df['category'].empty:
                categories = ['All'] + list(df['category'].unique())
                selected_category = st.selectbox("Category", categories)
                if selected_category != 'All':
                    df = df[df['category'] == selected_category]
            
            # Data display
            st.subheader("Product Overview")
            for _, row in df.iterrows():
                with st.expander(f"{row.get('title', 'Untitled')} - ${row.get('price', 'N/A')}"):
                    col1, col2 = st.columns([2, 1])
                    with col1:
                        if 'description' in row:
                            st.write("**Description:**")
                            st.write(row['description'])
                    with col2:
                        if 'rating' in row:
                            st.write("**Rating:**")
                            st.write(row['rating'])
                        if 'availability' in row:
                            st.write("**Availability:**")
                            st.write(row['availability'])
            
            # Statistics
            st.subheader("Statistics")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Products", len(df))
                if 'price' in df.columns and not df['price'].empty:
                    st.metric("Average Price", f"${df['price'].mean():.2f}")
            
            with col2:
                if 'price' in df.columns and not df['price'].empty:
                    st.metric("Highest Price", f"${df['price'].max():.2f}")
                    st.metric("Lowest Price", f"${df['price'].min():.2f}")
            
            # Charts
            if 'price' in df.columns and not df['price'].empty:
                st.subheader("Price Distribution")
                st.bar_chart(df['price'].value_counts().sort_index())
            
            if 'category' in df.columns and not df['category'].empty:
                st.subheader("Category Distribution")
                st.bar_chart(df['category'].value_counts())
            
            # Raw data table
            with st.expander("Raw Data"):
                st.dataframe(df)
    
    with tab3:
        st.header("Activity Log")
        logs = get_crawling_logs()
        
        if logs:
            for log in logs:
                timestamp = datetime.fromtimestamp(log['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                if log['type'] == 'visited':
                    st.info(f"[{timestamp}] Worker {log['worker_id']} visited: {log['url']}")
                else:
                    st.success(f"[{timestamp}] Worker {log['worker_id']} crawled: {log['url']} - {log.get('title', 'No title')} (${log.get('price', 'N/A')})")
        else:
            st.info("No activity recorded yet")

if __name__ == "__main__":
    main()
