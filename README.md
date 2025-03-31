# Distributed Web Scraping and Crawling System

A distributed web scraping and crawling system built with Python, using RabbitMQ for message queuing and MongoDB for data storage.

## Features

- Distributed crawling with multiple worker nodes
- Configurable crawling rules and data extraction
- Rate limiting and depth control
- Prometheus metrics for monitoring
- REST API for control and monitoring
- MongoDB for data storage
- BeautifulSoup4 for HTML parsing

## Prerequisites

- Python 3.8+
- RabbitMQ
- MongoDB
- Docker (optional, for containerization)

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
- MongoDB and RabbitMQ connection details

## Usage

1. Start the API server:
```bash
python api.py
```

2. Start one or more worker nodes:
```bash
python worker.py
```

3. Seed initial URLs using the scheduler:
```bash
python scheduler.py
```

## API Endpoints

- `/metrics` - Prometheus metrics
- `/stats` - Crawling statistics
- `/search?q=<query>` - Search crawled pages
- `/health` - Health check

## Monitoring

The system exposes Prometheus metrics that can be scraped by Prometheus and visualized using Grafana. Key metrics include:
- Pages crawled
- Crawl time
- Error count

## Docker Support

To run the system using Docker:

1. Build the images:
```bash
docker-compose build
```

2. Start the services:
```bash
docker-compose up -d
```

## Configuration

The `config.yaml` file contains all configuration options:

```yaml
crawler:
  max_depth: 3
  rate_limit: 1.0
  user_agent: "Mozilla/5.0 ..."
  allowed_domains: []
  start_urls: []

rabbitmq:
  host: "localhost"
  port: 5672
  username: "guest"
  password: "guest"
  queue_name: "crawler_queue"

mongodb:
  uri: "mongodb://localhost:27017"
  database: "web_crawler"
  collection: "pages"

extraction_rules:
  title:
    selector: "h1"
    type: "text"
  price:
    selector: ".price"
    type: "text"
```

## License

MIT License 