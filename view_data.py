import pymongo
from pprint import pprint

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["web_crawler"]
collection = db["pages"]

# Get all documents
print("\nCrawled Pages:")
print("-" * 50)
for doc in collection.find():
    print(f"\nURL: {doc.get('url', 'N/A')}")
    print(f"Depth: {doc.get('depth', 'N/A')}")
    print(f"Timestamp: {doc.get('timestamp', 'N/A')}")
    print("\nExtracted Data:")
    for key, value in doc.items():
        if key not in ['url', 'depth', 'timestamp', '_id']:
            print(f"{key}: {value}")
    print("-" * 50)

# Print summary
total_docs = collection.count_documents({})
print(f"\nTotal pages crawled: {total_docs}") 