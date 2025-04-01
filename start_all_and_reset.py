#!/usr/bin/env python3
import subprocess
import time
import requests
import json
import logging
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('start_all.log')
    ]
)
logger = logging.getLogger(__name__)

def run_command(command):
    """Run a shell command and log the output"""
    logger.info(f"Running command: {command}")
    try:
        result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
        logger.info(f"Command succeeded: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e}")
        logger.error(f"Error output: {e.stderr}")
        return False

def wait_for_service(url, max_attempts=30, delay=2):
    """Wait for a service to be available"""
    logger.info(f"Waiting for service at {url}...")
    for attempt in range(max_attempts):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                logger.info(f"Service at {url} is available")
                return True
            logger.info(f"Attempt {attempt+1}/{max_attempts}: Service not ready (status code {response.status_code})")
        except requests.RequestException:
            logger.info(f"Attempt {attempt+1}/{max_attempts}: Service not ready (connection error)")
        
        time.sleep(delay)
    
    logger.error(f"Service at {url} did not become available after {max_attempts} attempts")
    return False

def stop_worker():
    """Stop the worker if it's running"""
    logger.info("Stopping worker if running...")
    try:
        response = requests.post(
            "http://localhost:5000/stop",
            headers={"Content-Type": "application/json"},
            data=json.dumps({})
        )
        
        if response.status_code == 200:
            logger.info("Worker stopped successfully")
            # Give the worker time to fully shut down
            time.sleep(3)
            return True
        else:
            logger.error(f"Failed to stop worker: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Error stopping worker: {e}")
        return False

def reset_database():
    """Reset the database by calling the worker's reset_all endpoint"""
    logger.info("Resetting database...")
    try:
        # First, make sure the worker is stopped to avoid race conditions
        stop_worker()
        
        response = requests.post(
            "http://localhost:5000/reset_all",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"confirm": True})
        )
        
        if response.status_code == 200:
            logger.info("Database reset successful")
            time.sleep(2)  # Give the system time to fully process the reset
            return True
        else:
            logger.error(f"Database reset failed: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Error resetting database: {e}")
        return False

def start_worker():
    """Start the worker"""
    logger.info("Starting worker...")
    try:
        response = requests.post(
            "http://localhost:5000/start",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"worker_id": "worker1"})
        )
        
        if response.status_code == 200:
            logger.info("Worker started successfully")
            return True
        else:
            logger.error(f"Failed to start worker: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Error starting worker: {e}")
        return False

def add_initial_url():
    """Add an initial URL to crawl"""
    logger.info("Adding initial URL to crawl...")
    try:
        response = requests.post(
            "http://localhost:5001/add_job",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"url": "http://books.toscrape.com"})
        )
        
        if response.status_code == 200:
            logger.info("Initial URL added successfully")
            return True
        else:
            logger.error(f"Failed to add initial URL: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Error adding initial URL: {e}")
        return False

def main():
    """Main function to start all services with a clean database"""
    logger.info("Starting distributed web crawler with clean database...")
    
    # Check if Docker is running
    if not run_command("docker ps"):
        logger.error("Docker is not running or not accessible")
        return False
    
    # Start all services using docker-compose
    logger.info("Starting all services with docker-compose...")
    if not run_command("docker-compose up -d"):
        logger.error("Failed to start services with docker-compose")
        return False
    
    # Wait for services to be ready
    time.sleep(10)  # Give services some time to initialize
    
    # Check if the worker is up
    if not wait_for_service("http://localhost:5000/status"):
        logger.error("Worker service did not become available")
        return False
    
    # Reset the database
    if not reset_database():
        logger.error("Failed to reset database")
        return False
    
    # Start the worker
    if not start_worker():
        logger.error("Failed to start worker")
        return False
    
    # Wait for scheduler to be ready
    if not wait_for_service("http://localhost:5001/status"):
        logger.error("Scheduler service did not become available")
        return False
    
    # Add an initial URL to crawl
    if not add_initial_url():
        logger.error("Failed to add initial URL")
        return False
    
    logger.info("All services started successfully with a clean database!")
    logger.info("The system is now ready to use.")
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        logger.error("Failed to start all services with a clean database")
        sys.exit(1)
    sys.exit(0) 