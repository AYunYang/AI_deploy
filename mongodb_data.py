from pymongo import MongoClient
import json
from bson import json_util
import os
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Access the variables
mongo_ip = os.getenv("MONGO_IP")
databasename = os.getenv("DATABASE_NAME")


# Replace with your MongoDB connection string
connection_string = f"mongodb://User:securepassword@{mongo_ip}/?authSource={databasename}"
def debug_log(message):
    print(f"[DEBUG] {message}")

def fetch_data():
    try:
        # Establish connection to MongoDB
        debug_log("Attempting to connect to MongoDB.")
        client = MongoClient(connection_string)
        debug_log("MongoDB connection successful.")
        
        # Access the databases and collections
        debug_log("Accessing databases and collections.")
        w512_db = client['database_1']
        aircon_status_collection_1 = w512_db['w512_aircon_status']
        readings_collection_1 = w512_db['w512_readings']
        weather_data_collection = w512_db['weather_data']

        aircon_status_collection_2 = w512_db['spgg_aircon_status']
        readings_collection_2 = w512_db['spgg_readings']

        
        def fetch_all_as_json(collection, collection_name):
            try:
                debug_log(f"Fetching documents from collection: {collection_name}")
                data = list(collection.find())
                print()
        
                for document in data:
                    if "FC_FullStatus_Readings" in document:
                        readings = document["FC_FullStatus_Readings"]
        
                        # Check if readings is not None before iterating
                        if readings is not None and "FC_Unit_0" in readings:
                            updated_readings = {}
        
                            for key, value in readings.items():
                                old_index = int(key.split("_")[-1])
                                new_index = old_index + 1
                                new_key = f"FC_Unit_{new_index}"
                                updated_readings[new_key] = value
                            document["FC_FullStatus_Readings"] = updated_readings
        
                print("successful change")
                data_json = json.dumps(data, default=json_util.default)
        
                debug_log(f"Successfully fetched and converted {collection_name} to JSON.")
                return data_json
            except Exception as e:
                debug_log(f"Error fetching or converting {collection_name}: {e}")
                return None
            
        # Fetch data and check for issues
        aircon_status_json_1 = fetch_all_as_json(aircon_status_collection_1, "w512_aircon_status")
        readings_json_1 = fetch_all_as_json(readings_collection_1, "w512_readings")
        weather_data_json = fetch_all_as_json(weather_data_collection, "weather_data")
        
        aircon_status_json_2 = fetch_all_as_json(aircon_status_collection_2, "spgg_aircon_status")
        readings_json_2 = fetch_all_as_json(readings_collection_2, "spgg_readings")
        # Ensure output directory exists
        output_dir = "database_data"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to JSON files (with error handling)
        def save_to_file(data, file_path):
            try:
                debug_log(f"Saving data to {file_path}")
                with open(file_path, "w") as file:
                    file.write(data)
                debug_log(f"Successfully saved data to {file_path}")
            except Exception as e:
                debug_log(f"Error saving file {file_path}: {e}")
        # W512 site
        if aircon_status_json_1:
            save_to_file(aircon_status_json_1, os.path.join(output_dir, "W512_aircon_status.json"))
        if readings_json_1:
            save_to_file(readings_json_1, os.path.join(output_dir, "W512_readings.json"))
        # Weather data
        if weather_data_json:
            save_to_file(weather_data_json, os.path.join(output_dir, "Weather_data.json"))
        
        # SPGG
        if aircon_status_json_2:
            save_to_file(aircon_status_json_2, os.path.join(output_dir, "SPGG_aircon_status.json"))
        if readings_json_2:
            save_to_file(readings_json_2, os.path.join(output_dir, "SPGG_readings.json"))



        # Close the MongoDB connection
        debug_log("Closing MongoDB connection.")
        client.close()

    except Exception as e:
        debug_log(f"Error connecting to MongoDB or executing operations")