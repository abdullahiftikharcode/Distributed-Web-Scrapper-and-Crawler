import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["web_crawler"]
collection = db["pages"]

# Clear all documents
result = collection.delete_many({})
print(f"Cleared {result.deleted_count} documents from the collection") 