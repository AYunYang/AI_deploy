from pymongo import MongoClient
import json
from bson import json_util
import os

# Replace with your MongoDB connection string
pem_key = "pem_key/BMS_server.pem"
connection_string = "mongodb://User:securepassword@13.213.6.26:27017/?authSource=database_1"
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
        aircon_status_collection = w512_db['w512_aircon_status']
        readings_collection = w512_db['w512_readings']
        weather_data_collection = w512_db['weather_data']

        
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
        aircon_status_json = fetch_all_as_json(aircon_status_collection, "w512_aircon_status")
        readings_json = fetch_all_as_json(readings_collection, "w512_readings")
        weather_data_json = fetch_all_as_json(weather_data_collection, "weather_data")
        
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
        
        if aircon_status_json:
            save_to_file(aircon_status_json, os.path.join(output_dir, "W512_aircon_status.json"))
        if readings_json:
            save_to_file(readings_json, os.path.join(output_dir, "W512_readings.json"))
        if weather_data_json:
            save_to_file(weather_data_json, os.path.join(output_dir, "Weather_data.json"))
        
        # # Print JSON to console for debugging (optional)
        # debug_log("Debugging: Printing fetched JSON data to the console.")
        # print("\nAircon Status (JSON):", aircon_status_json)
        # print("\nW512 Readings (JSON):", readings_json)
        # print("\nWeather Data (JSON):", weather_data_json)
        
        # Close the MongoDB connection
        debug_log("Closing MongoDB connection.")
        client.close()

    except Exception as e:
        debug_log(f"Error connecting to MongoDB or executing operations")