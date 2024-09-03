import os
import json
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# Function to get input from the user
def get_credentials():
    username = input("Enter your username: ")
    password = input("Enter your password: ")
    return username, password

# Function to establish connection
def connect_to_mongo(username, password):
    uri = f"mongodb+srv://{username}:{password}@cluster-datafiniti.voasry5.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-datafiniti"
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(e)
        return None

# Function to get identifier type and value
def get_identifier():
    identifiers = {
        1: "ean",
        2: "ean13",
        3: "upc",
        4: "upca",
        5: "asins"
    }
    print("\nSelect Identifier:")
    for key, value in identifiers.items():
        print(f"{key} - {value}")

    choice = int(input("\nEnter the number corresponding to your choice: "))
    identifier = identifiers.get(choice)

    if identifier in ["ean", "ean13"]:
        value = input(f"Enter {identifier} (13 digits numeric): ")
        if not value.isdigit() or len(value) != 13:
            raise ValueError("Please provide a 13-digit numeric value.")
    elif identifier in ["upc", "upca"]:
        value = input(f"Enter {identifier} (12 digits numeric): ")
        if not value.isdigit() or len(value) != 12:
            raise ValueError("Please provide a 12-digit numeric value.")
    elif identifier == "asins":
        value = input(f"Enter {identifier} (10 digits alphanumeric): ")
        if not value.isalnum() or len(value) != 10:
            raise ValueError("Please provide a 10-digit alphanumeric value.")

    return identifier, value

# Function to get collection selection
def get_collection_selection():
    collections = [
        "extracted_features",
        "id_jsons",
        "latest_pricing_per_merchant",
        "latest_pricing_per_merchant_2024",
        "pricing_history",
        "updated_descriptions_jsons",
        "cleaned_reviews",
        "imageURLs_JSONs"
    ]
    
    print("\nSelect Collection to Fetch Data From:")
    for i, collection in enumerate(collections, 1):
        print(f"{i} - {collection}")
    print(f"{len(collections) + 1} - All Collections")
    
    choice = int(input("\nEnter the number corresponding to your choice: "))
    if choice == len(collections) + 1:
        return collections  # Return all collections
    else:
        return [collections[choice - 1]]  # Return the selected collection

# Function to fetch data from collections
def fetch_data(db, identifier, value, selected_collections):
    output_dir = "Output"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for collection in selected_collections:
        results = list(db[collection].find({identifier: value}))
        if results:
            collection_dir = os.path.join(output_dir, collection)
            if not os.path.exists(collection_dir):
                os.mkdir(collection_dir)
            with open(os.path.join(collection_dir, f"{value}.json"), "w") as outfile:
                json.dump(results, outfile, default=str)

def main():
    username, password = get_credentials()
    client = connect_to_mongo(username, password)
    if not client:
        return

    db = client['clean_data']
    identifier, value = get_identifier()
    selected_collections = get_collection_selection()
    fetch_data(db, identifier, value, selected_collections)
    print("Data fetched and stored in 'Output' folder.")

if __name__ == "__main__":
    main()