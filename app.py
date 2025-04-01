import streamlit as st
import pymongo
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os
import subprocess
import yaml
import time
import requests
import psutil
import traceback
import logging
import pika

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
if 'num_workers' not in st.session_state:
    st.session_state.num_workers = 3
if 'scheduler_url' not in st.session_state:
    st.session_state.scheduler_url = "http://scheduler:5001"
if 'worker_urls' not in st.session_state:
    st.session_state.worker_urls = ["http://worker:5000"]
if 'confirm_start' not in st.session_state:
    st.session_state.confirm_start = False
if 'show_worker_input' not in st.session_state:
    st.session_state.show_worker_input = False
if 'worker_count' not in st.session_state:
    st.session_state.worker_count = 1

def check_rabbitmq():
    try:
        logger.info("Attempting to connect to RabbitMQ...")
        host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
        port = int(os.getenv('RABBITMQ_PORT', '5672'))
        username = os.getenv('RABBITMQ_USER', 'admin')
        password = os.getenv('RABBITMQ_PASS', 'admin')
        
        logger.info(f"RabbitMQ Connection Details:")
        logger.info(f"Host: {host}")
        logger.info(f"Port: {port}")
        logger.info(f"Username: {username}")
        
        # Add a delay before first connection attempt
        logger.info("Waiting for RabbitMQ to be fully ready...")
        time.sleep(5)
        
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                port=port,
                credentials=pika.PlainCredentials(username, password),
                heartbeat=600,
                blocked_connection_timeout=300,
                connection_attempts=3,
                retry_delay=5,
                socket_timeout=5
            )
        )
        logger.info("Successfully connected to RabbitMQ")
        connection.close()
        logger.info("Successfully closed RabbitMQ connection")
        return True
    except Exception as e:
        logger.error(f"Error checking RabbitMQ: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return False

def check_mongodb_installation():
    """Check if MongoDB is installed and running"""
    try:
        uri = os.getenv('MONGODB_URI', 'mongodb://mongodb:27017/')
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.server_info()
        client.close()
        return True
    except pymongo.errors.ServerSelectionTimeoutError:
        st.error("MongoDB is not running. Please start MongoDB first.")
        st.error("To start MongoDB, open a new terminal and run: mongod")
        return False
    except pymongo.errors.ConnectionFailure:
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
        'scheduler': False,
        'workers': []
    }
    
    try:
        # Check scheduler status
        response = requests.get(f"{st.session_state.scheduler_url}/status")
        if response.status_code == 200:
            scheduler_status = response.json()
            status['scheduler'] = scheduler_status.get('is_running', False)
        
        # Check worker statuses
        for worker_url in st.session_state.worker_urls:
            try:
                response = requests.get(f"{worker_url}/status")
                if response.status_code == 200:
                    worker_status = response.json()
                    if worker_status.get('is_running', False):
                        status['workers'].append(worker_url)
            except Exception as e:
                logger.error(f"Error checking worker status at {worker_url}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error checking service status: {str(e)}")
    
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
        uri = os.getenv('MONGODB_URI', 'mongodb://mongodb:27017/')
        client = pymongo.MongoClient(uri)
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

def start_all_services():
    """Start all services using their respective APIs"""
    try:
        logger.info("Starting all services...")
        success = True
        
        # Start scheduler
        logger.info(f"Starting scheduler at {st.session_state.scheduler_url}")
        try:
            response = requests.post(
                f"{st.session_state.scheduler_url}/start",
                timeout=5,
                verify=False
            )
            if response.status_code != 200:
                error_msg = f"Error starting scheduler: {response.text}"
                logger.error(error_msg)
                st.error(error_msg)
                success = False
            else:
                logger.info("Successfully started scheduler")
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error starting scheduler: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Full traceback: {traceback.format_exc()}")
            st.error(error_msg)
            success = False
        except requests.exceptions.Timeout as e:
            error_msg = f"Timeout error starting scheduler: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Full traceback: {traceback.format_exc()}")
            st.error(error_msg)
            success = False
        except Exception as e:
            error_msg = f"Error starting scheduler: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Full traceback: {traceback.format_exc()}")
            st.error(error_msg)
            success = False
            
        # Start workers
        logger.info(f"Starting {len(st.session_state.worker_urls)} workers...")
        for i, worker_url in enumerate(st.session_state.worker_urls):
            try:
                logger.info(f"Starting worker {i+1} at {worker_url}")
                worker_id = f"worker_{i+1}"
                response = requests.post(
                    f"{worker_url}/start",
                    json={"worker_id": worker_id},
                    timeout=5,
                    verify=False
                )
                if response.status_code != 200:
                    error_msg = f"Error starting worker {worker_id}: {response.text}"
                    logger.error(error_msg)
                    st.error(error_msg)
                    success = False
                else:
                    logger.info(f"Successfully started worker {worker_id}")
            except requests.exceptions.ConnectionError as e:
                error_msg = f"Connection error starting worker {i+1}: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Full traceback: {traceback.format_exc()}")
                st.error(error_msg)
                success = False
            except requests.exceptions.Timeout as e:
                error_msg = f"Timeout error starting worker {i+1}: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Full traceback: {traceback.format_exc()}")
                st.error(error_msg)
                success = False
            except Exception as e:
                error_msg = f"Error starting worker {i+1}: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Full traceback: {traceback.format_exc()}")
                st.error(error_msg)
                success = False
                
        return success
    except Exception as e:
        error_msg = f"Error starting services: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Full traceback: {traceback.format_exc()}")
        st.error(error_msg)
        return False

def stop_all_services():
    """Stop all running services using their respective APIs"""
    try:
        logger.info("Stopping all services...")
        success = True
        
        # Stop scheduler
        try:
            response = requests.post(f"{st.session_state.scheduler_url}/stop")
            if response.status_code != 200:
                error_msg = f"Error stopping scheduler: {response.text}"
                logger.error(error_msg)
                st.error(error_msg)
                success = False
            else:
                logger.info("Successfully stopped scheduler")
        except Exception as e:
            error_msg = f"Error stopping scheduler: {str(e)}"
            logger.error(error_msg)
            st.error(error_msg)
            success = False
        
        # Stop workers
        for i, worker_url in enumerate(st.session_state.worker_urls):
            try:
                response = requests.post(f"{worker_url}/stop")
                if response.status_code != 200:
                    error_msg = f"Error stopping worker {i+1}: {response.text}"
                    logger.error(error_msg)
                    st.error(error_msg)
                    success = False
                else:
                    logger.info(f"Successfully stopped worker {i+1}")
            except Exception as e:
                error_msg = f"Error stopping worker {i+1}: {str(e)}"
                logger.error(error_msg)
                st.error(error_msg)
                success = False
        
        # Clear session state
        st.session_state.worker_urls = []
        st.session_state.confirm_start = False
        st.session_state.show_worker_input = False
        
        if success:
            logger.info("All services stopped successfully")
            st.success("Successfully stopped all services!")
        return success
            
    except Exception as e:
        logger.error(f"Error stopping services: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        st.error(f"Error stopping services: {str(e)}")
        return False

def check_api_status():
    try:
        # Try to connect to the API endpoint
        api_url = os.getenv('API_URL', 'http://api:5002')
        response = requests.get(f"{api_url}/health", timeout=2)
        if response.status_code == 200:
            return True
            
        # Fall back to process check if HTTP request fails
        for proc in psutil.process_iter(['name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info.get('cmdline', []))
                if 'python' in proc.info.get('name', '') and ('api_server.py' in cmdline or 'api.py' in cmdline):
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return False
    except Exception as e:
        logger.error(f"Error checking API status: {str(e)}")
        return False

def get_worker_details():
    try:
        worker_stats = {}
        for i, worker_url in enumerate(st.session_state.worker_urls):
            try:
                response = requests.get(f"{worker_url}/stats")
                if response.status_code == 200:
                    stats = response.json()
                    stats['worker_id'] = str(i + 1)
                    worker_stats[str(i + 1)] = stats
            except Exception as e:
                logger.error(f"Error getting stats for worker {i+1}: {str(e)}")
        return worker_stats
    except Exception as e:
        st.error(f"Error getting worker details: {str(e)}")
        return {}

def get_queue_stats():
    try:
        response = requests.get(f"{st.session_state.scheduler_url}/queue_stats")
        if response.status_code == 200:
            return response.json()
        return {
            'pending_count': 0,
            'processing_count': 0,
            'completed_count': 0,
            'failed_count': 0
        }
    except Exception as e:
        st.error(f"Error getting queue stats: {str(e)}")
        return {
            'pending_count': 0,
            'processing_count': 0,
            'completed_count': 0,
            'failed_count': 0
        }

def get_crawling_logs():
    try:
        client = pymongo.MongoClient("mongodb://mongodb:27017/")
        db = client["web_crawler"]
        visited_urls = list(db.visited_urls.find().sort("timestamp", -1).limit(50))
        pages = list(db.pages.find().sort("timestamp", -1).limit(50))
        logs = []
        for url in visited_urls:
            logs.append({
                'timestamp': url['timestamp'],
                'type': 'visited',
                'url': url['url'],
                'worker_id': url.get('worker_id', 'unknown')
            })
        for page in pages:
            logs.append({
                'timestamp': page['timestamp'],
                'type': 'crawled',
                'url': page['url'],
                'title': page.get('title', 'No title'),
                'price': page.get('price', 'N/A'),
                'worker_id': page.get('worker_id', 'unknown')
            })
        logs.sort(key=lambda x: x['timestamp'], reverse=True)
        return logs
    except Exception as e:
        st.error(f"Error getting crawling logs: {str(e)}")
        return []

def scale_workers(num_workers):
    """Scale the number of workers up or down"""
    try:
        # Get current worker count
        current_workers = len(st.session_state.get('worker_urls', []))
        
        if num_workers > current_workers:
            # Scale up
            for i in range(current_workers, num_workers):
                worker_url = f"http://localhost:{5000 + i}"
                try:
                    response = requests.post(
                        f"{worker_url}/start",
                        json={'worker_id': str(i + 1)}
                    )
                    if response.status_code != 200:
                        error_msg = f"Error starting worker {i+1}: {response.text}"
                        logger.error(error_msg)
                        st.error(error_msg)
                    else:
                        logger.info(f"Successfully started worker {i+1}")
                except Exception as e:
                    error_msg = f"Error starting worker {i+1}: {str(e)}"
                    logger.error(error_msg)
                    st.error(error_msg)
        elif num_workers < current_workers:
            # Scale down
            for i in range(current_workers - 1, num_workers - 1, -1):
                worker_url = f"http://localhost:{5000 + i}"
                try:
                    response = requests.post(f"{worker_url}/stop")
                    if response.status_code != 200:
                        error_msg = f"Error stopping worker {i+1}: {response.text}"
                        logger.error(error_msg)
                        st.error(error_msg)
                except Exception as e:
                    error_msg = f"Error stopping worker {i+1}: {str(e)}"
                    logger.error(error_msg)
                    st.error(error_msg)
        
        st.success(f"Successfully scaled to {num_workers} workers")
        return True
    except Exception as e:
        st.error(f"Error scaling workers: {str(e)}")
        return False

def main():
    st.title("🕷️ Distributed Web Crawler Dashboard")
    
    # Add refresh button at the top
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.success("Data refreshed!")
        st.rerun()
    
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
    
    if check_api_status():
        st.sidebar.success("API Server: Running")
    else:
        st.sidebar.warning("API Server: Not Running")
    
    if status['scheduler']:
        st.sidebar.success("Scheduler: Running")
    else:
        st.sidebar.warning("Scheduler: Not Running")
    
    num_workers = len(status['workers'])
    if num_workers > 0:
        st.sidebar.success(f"Workers: {num_workers} Running")
    else:
        st.sidebar.warning("Workers: Not Running")
    
    st.sidebar.markdown("---")
    st.sidebar.header("Worker Controls")
    
    # Worker scaling controls
    if status['scheduler']:  # Only show scaling controls if scheduler is running
        current_workers = len(status['workers'])
        # Ensure current_workers is at least 1 to avoid the StreamlitAPIException
        current_workers = max(1, current_workers)
        new_worker_count = st.sidebar.number_input(
            "Number of Workers",
            min_value=1,
            max_value=10,
            value=current_workers,
            help="Adjust the number of worker nodes (1-10)"
        )
        
        if new_worker_count != current_workers:
            if st.sidebar.button("Scale Workers"):
                if scale_workers(new_worker_count):
                    st.rerun()
    else:
        st.sidebar.info("Start the scheduler first to enable worker scaling")
    
    st.sidebar.markdown("---")
    st.sidebar.header("Worker Details")
    worker_stats = get_worker_details()
    
    if worker_stats:
        for worker_id, stats in worker_stats.items():
            with st.sidebar.expander(f"Worker {worker_id}"):
                st.metric("Pages Crawled", stats['pages_crawled'])
                st.metric("Current Depth", stats['max_depth'])
                if stats['last_url']:
                    st.text(f"Last URL: {stats['last_url']}")
                if stats['last_timestamp']:
                    st.text(f"Last Activity: {datetime.fromtimestamp(stats['last_timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")
                if stats['errors'] > 0:
                    st.error(f"Errors: {stats['errors']}")
    else:
        st.sidebar.info("No worker activity recorded yet")
    
    st.sidebar.markdown("---")
    st.sidebar.header("Service Controls")
    url = st.sidebar.text_input("Enter URL to scrape", "http://books.toscrape.com/")
    st.session_state['url'] = url
    
    # Service control buttons
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        # Initial "Start All Services" button
        if not st.session_state.confirm_start:
            if st.button("Start All Services", type="primary", key="start_services"):
                logger.info("Start All Services button clicked")
                # Check service status before proceeding
                status = check_service_status()
                if not status['mongodb']:
                    st.error("MongoDB is not running. Please start MongoDB first.")
                    return
                if not status['rabbitmq']:
                    st.error("RabbitMQ is not running. Please start RabbitMQ first.")
                    return
                    
                st.session_state.confirm_start = True
                st.session_state.show_worker_input = True
                st.rerun()
        
        # Worker count input and confirmation button
        elif st.session_state.show_worker_input:
            st.session_state.worker_count = st.number_input(
                "How many worker nodes?",
                min_value=1,
                max_value=10,
                value=st.session_state.worker_count,
                key="worker_count_input"
            )
            
            # Confirm and Start All Services button
            if st.button("Confirm and Start All Services", type="secondary", key="confirm_services"):
                logger.info("Confirm and Start All Services button clicked")
                # Double check service status
                status = check_service_status()
                if not status['mongodb']:
                    st.error("MongoDB is not running. Please start MongoDB first.")
                    st.session_state.confirm_start = False
                    st.session_state.show_worker_input = False
                    st.rerun()
                    return
                if not status['rabbitmq']:
                    st.error("RabbitMQ is not running. Please start RabbitMQ first.")
                    st.session_state.confirm_start = False
                    st.session_state.show_worker_input = False
                    st.rerun()
                    return
                    
                # Start all services
                logger.info(f"Starting services with {st.session_state.worker_count} workers")
                if start_all_services():
                    logger.info("Services started successfully")
                    st.session_state.confirm_start = False
                    st.session_state.show_worker_input = False
                    st.rerun()
                else:
                    logger.error("Failed to start services")
                    st.session_state.confirm_start = False
                    st.session_state.show_worker_input = False
                    st.rerun()
    
    with col2:
        # Stop All Services button
        if st.button("Stop All Services", type="secondary", key="stop_services"):
            logger.info("Stop All Services button clicked")
            if stop_all_services():
                logger.info("Services stopped successfully")
                st.success("All services stopped successfully!")
                st.session_state.confirm_start = False
                st.session_state.show_worker_input = False
                st.rerun()
            else:
                logger.error("Failed to stop services")
                st.error("Failed to stop services")
                
    st.sidebar.markdown("---")
    st.sidebar.header("Data Display")
    df = load_data()
    
    if df.empty:
        st.info("No valid data available. Please make sure the crawler has collected some data with valid prices.")
        st.stop()
    
    st.sidebar.header("Filters")
    if 'price' in df.columns and len(df) > 0:
        min_price = float(df['price'].min())
        max_price = float(df['price'].max())
        
        if min_price == max_price:
            st.sidebar.text(f"Fixed Price: ${min_price:.2f}")
        else:
            price_range = st.sidebar.slider(
                "Price Range ($)",
                min_value=min_price,
                max_value=max_price,
                value=(min_price, max_price),
                step=0.01
            )
            df = df[(df['price'] >= price_range[0]) & (df['price'] <= price_range[1])]
    
    if 'category' in df.columns and not df['category'].empty:
        categories = ['All'] + list(df['category'].unique())
        selected_category = st.sidebar.selectbox("Category", categories)
        if selected_category != 'All':
            df = df[df['category'] == selected_category]
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Product Overview")
        for _, row in df.iterrows():
            with st.expander(f"{row.get('title', 'Untitled')} - ${row.get('price', 'N/A')}"):
                col1, col2 = st.columns([2, 1])
                with col1:
                    if 'description' in row:
                        st.write("**Description:**")
                        st.write(row['description'])
                with col2:
                    if 'ratings' in row:
                        st.write("**Ratings:**")
                        st.write(row['ratings'])
                    if 'reviews' in row:
                        st.write("**Reviews:**")
                        st.write(row['reviews'])
    
    with col2:
        st.subheader("Statistics")
        st.metric("Total Products", len(df))
        if 'price' in df.columns and not df['price'].empty:
            st.metric("Average Price", f"${df['price'].mean():.2f}")
            st.metric("Highest Price", f"${df['price'].max():.2f}")
            st.metric("Lowest Price", f"${df['price'].min():.2f}")
        if 'price' in df.columns and not df['price'].empty:
            st.write("**Price Distribution**")
            st.bar_chart(df['price'].value_counts().sort_index())
        if 'category' in df.columns and not df['category'].empty:
            st.write("**Category Distribution**")
            st.bar_chart(df['category'].value_counts())
    
    st.subheader("Raw Data")
    st.dataframe(df)
    
    st.subheader("Crawling Activity Logs")
    logs = get_crawling_logs()
    
    if logs:
        for log in logs:
            timestamp = datetime.fromtimestamp(log['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            if log['type'] == 'visited':
                st.info(f"[{timestamp}] Worker {log['worker_id']} visited: {log['url']}")
            else:
                st.success(f"[{timestamp}] Worker {log['worker_id']} crawled: {log['url']} - {log.get('title', 'No title')} (${log.get('price', 'N/A')})")
    else:
        st.info("No crawling activity recorded yet")

if __name__ == "__main__":
    main()
