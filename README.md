# Distributed Web Scraping and Crawling System

A distributed web scraping and crawling system built with Python, using RabbitMQ for message queuing, MongoDB for data storage, and a custom network protocol for distributed worker coordination.

## Features

- **True distributed architecture**: Workers can connect from any machine
- **Central server coordination**: Single source of truth for task distribution
- **Configurable crawling rules** and data extraction
- **Heartbeat monitoring** for worker health tracking
- **Rate limiting** and depth control
- **Prometheus metrics** for monitoring
- **MongoDB** for data storage
- **BeautifulSoup4** for HTML parsing
- **Streamlit dashboard** for visualization and control

## System Architecture

The system consists of three main components:

1. **Central Server**: Coordinates all crawling activity
   - Maintains worker registry
   - Distributes URLs to workers
   - Tracks worker status and health
   - Stores results in MongoDB

2. **Remote Workers**: Connect to the central server
   - Process URLs
   - Extract data from web pages
   - Find and return new links
   - Maintain heartbeats with the server

3. **Streamlit Dashboard**: Provides UI for system control
   - Start/stop the central server
   - Monitor worker status
   - View crawling results in real-time
   - Seed initial URLs

## Prerequisites

- Python 3.8+
- RabbitMQ
- MongoDB
- Network connectivity between server and workers

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd distributed-web-crawler
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure the crawler:
Edit `config.yaml` to set your crawling parameters, including:
- Start URLs
- Allowed domains
- Extraction rules

## Usage

### Starting the Central Server

1. Start the Streamlit dashboard:
```bash
streamlit run app.py
```

2. In the dashboard:
   - Ensure MongoDB and RabbitMQ are running (green status indicators)
   - Set server host/port in the server configuration panel (default: 0.0.0.0:5555)
   - Click "Start Server"
   - Click "Seed URL" to add the initial URL to the crawl queue

### Connecting Remote Workers

On each worker machine:

1. Copy the `remote_worker.py` file and the `requirements.txt` file
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Start the worker, connecting to the central server:
```bash
python remote_worker.py --server <server-ip> --port <server-port>
```

For example:
```bash
python remote_worker.py --server 192.168.1.100 --port 5555
```

You can also assign a custom worker ID:
```bash
python remote_worker.py --server 192.168.1.100 --port 5555 --id worker-laptop-1
```

### Monitoring the System

1. Use the Streamlit dashboard to:
   - View active workers and their status
   - Monitor the URL queue
   - View real-time crawling statistics
   - Browse extracted data

2. Check the "Dashboard" tab for:
   - System service status
   - Worker counts
   - Queue statistics
   - Pending URLs

3. Check the "Crawled Data" tab for:
   - Extracted product information
   - Price statistics
   - Category distribution

4. Check the "Activity Log" tab for:
   - Real-time crawling activity
   - Worker actions and completed tasks

## Communication Protocol

The workers and server communicate using a simple message protocol over TCP sockets:

- Each message is prefixed with a 4-byte message length
- Message bodies are JSON-encoded
- Messages include a 'type' field indicating the message purpose

Message types include:
- `register`: Worker registration with the server
- `heartbeat`: Regular health check from worker
- `request_task`: Worker requesting a URL to process
- `task_complete`: Worker reporting completed task

## Configuration

The `config.yaml` file contains all configuration options:

```yaml
crawler:
  max_depth: 3
  rate_limit: 1.0
  user_agent: "Mozilla/5.0 ..."
  allowed_domains: 
    - books.toscrape.com
  start_url: "http://books.toscrape.com/"

extraction_rules:
  title:
    selector: ".product_main h1"
    type: "text"
  price:
    selector: ".product_main .price_color"
    type: "text"
```

## Troubleshooting

- **Worker can't connect**: Check network connectivity and firewall settings
- **No data being crawled**: Verify the initial URL has been seeded
- **Worker disconnecting**: Ensure stable network connection between worker and server

## Security Considerations

- This system uses plain TCP sockets without encryption
- For production use, consider implementing TLS/SSL
- Implement authentication if deployed on public networks

## License

MIT License 