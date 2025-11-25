from pymongo import MongoClient
import urllib.parse

# MongoDB Atlas credentials
username = "adityabhoir983_db_user"
password = "HiV2rwczhpH0Cpjq"
encoded_password = urllib.parse.quote_plus(password)

# Final connection string
connection_string = (
    f"mongodb+srv://{username}:{encoded_password}"
    "@cluster0.aavnxbi.mongodb.net/pharmacy_db"
    "?retryWrites=true&w=majority&appName=Cluster0"
)

client = MongoClient(connection_string)

# Choose DB and Collection
db = client["pharmacy_db"]
users_collection = db["user"]   # change if your collection name is different

# The document you want to add
document = {
    "username": "adityabhoir983",
    "pass": "adi@123",
    "email": "adityabhoir983@gmail.com"
}

# Insert data
result = users_collection.insert_one(document)

print("Inserted ID:", result.inserted_id)
