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
import pika
import psutil
import traceback
import logging

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
if 'api_process' not in st.session_state:
    st.session_state.api_process = None
if 'scheduler_process' not in st.session_state:
    st.session_state.scheduler_process = None
if 'worker_processes' not in st.session_state:
    st.session_state.worker_processes = []
if 'confirm_start' not in st.session_state:
    st.session_state.confirm_start = False
if 'show_worker_input' not in st.session_state:
    st.session_state.show_worker_input = False
if 'worker_count' not in st.session_state:
    st.session_state.worker_count = 3

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
    
    # Check scheduler
    for proc in psutil.process_iter(['name', 'cmdline']):
        try:
            if 'python' in proc.info['name'] and 'scheduler.py' in ' '.join(proc.info['cmdline']):
                status['scheduler'] = True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    
    # Check workers
    for proc in psutil.process_iter(['name', 'cmdline']):
        try:
            if 'python' in proc.info['name'] and 'worker.py' in ' '.join(proc.info['cmdline']):
                status['workers'].append(proc.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    
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

def start_worker(worker_id):
    try:
        env = os.environ.copy()
        env['WORKER_ID'] = str(worker_id)
        logger.info(f"Starting worker {worker_id}...")
        
        # Start worker process without capturing stdout/stderr
        process = subprocess.Popen(
            ['python', 'worker.py'],
            env=env,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        
        if process.poll() is None:
            logger.info(f"Worker {worker_id} started successfully with PID {process.pid}")
            return process.pid
        else:
            logger.error(f"Worker {worker_id} failed to start.")
            st.error(f"Worker {worker_id} failed to start.")
            return None
    except Exception as e:
        error_msg = f"Error starting worker {worker_id}: {str(e)}"
        logger.error(error_msg)
        st.error(error_msg)
        return None

def start_all_services():
    try:
        st.info("Starting all services...")
        if not check_mongodb_installation():
            st.error("MongoDB is not running. Please start MongoDB first.")
            return
        if not check_rabbitmq():
            st.error("RabbitMQ is not running. Please start RabbitMQ first.")
            return
            
        num_workers = st.session_state.get('worker_count', 3)
        st.info(f"Starting {num_workers} workers...")
        
        config = load_config()
        if not config:
            st.error("Failed to load configuration")
            return
            
        if 'crawler' not in config:
            config['crawler'] = {}
        config['crawler']['start_url'] = st.session_state.get('url', '')
        
        with open('config.yaml', 'w') as f:
            yaml.dump(config, f)
            
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client["web_crawler"]
            db.pages.delete_many({})
            db.visited_urls.delete_many({})
            db.url_queue.delete_many({})
            st.success("Cleared existing data")
        except Exception as e:
            st.error(f"Error clearing data: {str(e)}")
            return
            
        try:
            api_process = subprocess.Popen(
                ['python', 'api.py'],
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )
            st.session_state['api_process'] = api_process
            st.success("Started API server")
        except Exception as e:
            st.error(f"Error starting API server: {str(e)}")
            return
            
        try:
            # Start scheduler without capturing stdout/stderr
            scheduler_process = subprocess.Popen(
                ['python', 'scheduler.py'],
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )
            st.session_state['scheduler_process'] = scheduler_process
            st.success("Started scheduler")
            time.sleep(2)
            if scheduler_process.poll() is not None:
                st.error("Scheduler failed to start.")
                return
        except Exception as e:
            st.error(f"Error starting scheduler: {str(e)}")
            return
            
        worker_processes = []
        for i in range(num_workers):
            try:
                env = os.environ.copy()
                env['WORKER_ID'] = str(i)
                # Start worker process without capturing stdout/stderr
                worker_process = subprocess.Popen(
                    ['python', 'worker.py'],
                    env=env,
                    creationflags=subprocess.CREATE_NEW_CONSOLE
                )
                worker_processes.append(worker_process)
                st.success(f"Started worker {i}")
                st.write(f"Worker {i} process ID: {worker_process.pid}")
                time.sleep(2)  # Give worker time to start
                if worker_process.poll() is not None:
                    st.error(f"Worker {i} failed to start.")
                    return
            except Exception as e:
                st.error(f"Error starting worker {i}: {str(e)}")
                return
                
        st.session_state['worker_processes'] = worker_processes
        st.success("All services started successfully!")
        st.write("Process IDs:")
        st.write(f"API Server: {api_process.pid}")
        st.write(f"Scheduler: {scheduler_process.pid}")
        for i, proc in enumerate(worker_processes):
            st.write(f"Worker {i}: {proc.pid}")
            
    except Exception as e:
        st.error(f"Error starting services: {str(e)}")
        st.error(traceback.format_exc())

def stop_all_services():
    try:
        for proc in psutil.process_iter(['name', 'cmdline']):
            try:
                if 'python' in proc.info['name']:
                    cmdline = ' '.join(proc.info['cmdline'])
                    if 'scheduler.py' in cmdline or 'worker.py' in cmdline or 'api.py' in cmdline:
                        proc.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return True
    except Exception as e:
        st.error(f"Error stopping services: {str(e)}")
    return False

def check_api_status():
    try:
        for proc in psutil.process_iter(['name', 'cmdline']):
            try:
                if 'python' in proc.info['name'] and 'api.py' in ' '.join(proc.info['cmdline']):
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return False
    except Exception:
        return False

def get_worker_details():
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["web_crawler"]
        collection = db["pages"]
        documents = list(collection.find())
        worker_stats = {}
        for doc in documents:
            worker_id = doc.get('worker_id', 'unknown')
            if worker_id not in worker_stats:
                worker_stats[worker_id] = {
                    'pages_crawled': 0,
                    'max_depth': 0,
                    'last_url': None,
                    'last_timestamp': None,
                    'errors': 0
                }
            stats = worker_stats[worker_id]
            stats['pages_crawled'] += 1
            stats['max_depth'] = max(stats['max_depth'], doc.get('depth', 0))
            timestamp = doc.get('timestamp', 0)
            if stats['last_timestamp'] is None or timestamp > stats['last_timestamp']:
                stats['last_url'] = doc.get('url')
                stats['last_timestamp'] = timestamp
            if 'error' in doc:
                stats['errors'] += 1
        return worker_stats
    except Exception as e:
        st.error(f"Error getting worker details: {str(e)}")
        return {}

def get_crawling_logs():
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
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

def main():
    st.title("🕷️ Distributed Web Crawler Dashboard")
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
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if not st.session_state.confirm_start:
            if st.button("Start All Services", type="primary", key="start_services"):
                st.session_state.confirm_start = True
                st.session_state.show_worker_input = True
                st.write("Start All Services button clicked!")
                st.rerun()  # Force a rerun to update the UI
        else:
            if st.session_state.show_worker_input:
                st.session_state.worker_count = st.number_input(
                    "How many worker nodes do you want to create?",
                    min_value=1,
                    max_value=10,
                    value=st.session_state.worker_count,
                    key="worker_count_input"
                )
                if st.button("Confirm and Start All Services", type="secondary", key="confirm_services"):
                    st.write("Confirm button clicked! Starting services...")
                    if start_all_services():
                        st.success("All services started successfully!")
                        st.session_state.confirm_start = False
                        st.session_state.show_worker_input = False
                        st.rerun()  # Force a rerun to update the UI
                    else:
                        st.error("Failed to start services")
                        st.session_state.confirm_start = False
                        st.session_state.show_worker_input = False
                        st.rerun()  # Force a rerun to update the UI
    
    with col2:
        if st.button("Stop All Services", type="secondary", key="stop_services"):
            st.write("Stop All Services button clicked!")
            if stop_all_services():
                st.success("All services stopped successfully!")
                st.session_state.confirm_start = False
                st.session_state.show_worker_input = False
                st.rerun()  # Force a rerun to update the UI
            else:
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
