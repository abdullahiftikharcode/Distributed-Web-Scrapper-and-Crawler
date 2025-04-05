# Setting Up the Distributed Web Crawler

This document provides instructions for setting up and running the distributed web crawler.

## Prerequisites

- Python 3.8+
- MongoDB
- RabbitMQ

## Server Setup

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Ensure MongoDB and RabbitMQ are running on your system.

3. Start the Streamlit dashboard:
   ```bash
   streamlit run app.py
   ```

4. In the dashboard:
   - Verify MongoDB and RabbitMQ show as "Running" (green indicators)
   - Click "Start Server"
   - Click "Seed URL" to add the initial URL to the queue

## Worker Setup

### Configuring the Worker

The server host and port are hardcoded in the `remote_worker.py` file. Currently set to:
- Server Host: `127.0.0.1` (native machine loopback address)
- Server Port: `5555`

To change these values, edit lines 21-22 in `remote_worker.py`:

```python
# Hardcoded server configuration
SERVER_HOST = "your-server-ip"  # Change this to your server's IP address
SERVER_PORT = 5555
```

### Running Workers

1. Copy the `remote_worker.py` and `requirements.txt` files to each worker machine.

2. Install the required dependencies on each worker machine:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the worker:
   ```bash
   python remote_worker.py
   ```

Each worker will:
- Auto-generate a unique worker ID based on hostname
- Connect to the server at the hardcoded address
- Start processing URLs from the queue

## Monitoring

You can monitor the system through the Streamlit dashboard:

1. **Dashboard tab**: Shows system status, worker counts, and queue statistics
2. **Crawled Data tab**: Displays extracted data and statistics
3. **Activity Log tab**: Shows real-time crawling activity

## Troubleshooting

- **Worker can't connect to server**: When running on a single machine, 127.0.0.1 works fine. For distributed setups, update the SERVER_HOST to your actual server IP address
- **No data being crawled**: Verify that you've seeded the initial URL in the Streamlit dashboard
- **MongoDB/RabbitMQ errors**: Ensure these services are running on the server

## Network Configuration

For distributed setups where workers are on different machines:

1. Make sure the server is accessible on the network (check firewall settings)
2. The server must bind to an interface that's accessible to workers (default is `0.0.0.0`, which binds to all interfaces)
3. Use the server's actual network IP address, not `127.0.0.1`, in the worker configuration if running on different machines 