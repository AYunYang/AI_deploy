#imports
import requests
import json
import pandas as pd
import numpy as np
import warnings
import random
import matplotlib.pyplot as plt
from datetime import datetime
import time as clock


def execute_kmeans():


    Sensors_Data = pd.read_json('database_data/W512_readings.json')
    Sensors_Data_1 = pd.read_json('database_data/SPGG_readings.json') # Since w512 don have outdoor sensor yet, we will use the outdoor sensor from spgg
    Aircon_Data = pd.read_json('database_data/W512_aircon_status.json')
    Weather_Data = pd.read_json('database_data/Weather_data.json')

    Aircon_rows = []

    for _, row in Aircon_Data.iterrows():
        date = row['date']
        time = row['time']
        
        flattened_row = {
            "date": date,
            "time": time
        }
        
        fc_readings = row['FC_FullStatus_Readings']
        if fc_readings and isinstance(fc_readings, dict):
            for unit, data in fc_readings.items():
                if any(data.get("Set_Point", None) == 404.0 for data in fc_readings.values()):
                    continue
                flattened_row[f"{unit}_Status"] = data.get("Status", None)
                flattened_row[f"{unit}_Fan_Status"] = data.get("Fan_Status", None)
                flattened_row[f"{unit}_Set_Point"] = data.get("Set_Point", None)
                flattened_row[f"{unit}_Operation_Mode"] = data.get("Operation_Mode", None)
        
        Aircon_rows.append(flattened_row)

    Sensors_rows = []
    include_keys_1 = ["24E124725E285123", "24E124725E331695","24E124725E331744",
                        "24E124725E332483","24E124725E290348","24E124725E331733","24E124725E286745","24E124725E332564" # "24E124136D316361" is supposed to be outdoor but it is not outdoor yet
                            "24E124757E150866","24E124757E150896"]

    include_keys_2 = ["Sensor_1","Sensor_3","Sensor_6"]
    for _, row in Sensors_Data.iterrows():
        invalid_input = False
        
        date = row['date']
        time = row['time']
        
        flattened_row = {
            "date": date,
            "time": time
        }
        
        
        lorawan_readings = row['Lorawan_Readings']
        
        if isinstance(lorawan_readings, dict):
            for unit, data in lorawan_readings.items():
                if unit not in include_keys_1:
                    continue
                if isinstance(data, dict):  # Ensure that each item in Lorawan_Readings is a dictionary
                    for key, value in data.items():
                        
                        flattened_row[f"{unit}_{key}"] = value
                
        energy_readings = row['Energy_Readings']
        total_power = 0
        total_energy = 0
        invalid_input_power = False
        invalid_input_energy = False
        
        if energy_readings and isinstance(energy_readings, dict):
            for unit, data in energy_readings.items():
                if unit not in include_keys_2:
                    continue
                power = data.get('Power', None)
                energy = data.get('Energy', None)
                if power is None:
                    invalid_input_power = True
                if energy is None:
                    invalid_input_energy = True
                total_power += power
                total_energy += energy
            
        if invalid_input_power:
            total_power = None
        if invalid_input_energy:
            total_energy = None
            
        flattened_row["Total_Energy"] = total_energy
        flattened_row["Total_Power"] = total_power
        
        Sensors_rows.append(flattened_row)

    Sensors_rows_1 = []
    outdoor_key = ["24E124136D336145"] # this is the id of the outdoor sensor from spgg

    for _, row in Sensors_Data_1.iterrows():
        invalid_input = False
        
        date = row['date']
        time = row['time']
        
        flattened_row = {
            "date": date,
            "time": time
        }
        
        
        lorawan_readings = row['Lorawan_Readings']
        
        if isinstance(lorawan_readings, dict):
            for unit, data in lorawan_readings.items():
                if unit not in outdoor_key:
                    continue
                if isinstance(data, dict):  # Ensure that each item in Lorawan_Readings is a dictionary
                    for key, value in data.items():
                        flattened_row[f"{unit}_{key}"] = value

        Sensors_rows_1.append(flattened_row)


    # Normalize the data
    Weather_rows = []

    for _, row in Weather_Data.iterrows():
        date = row['date']
        time = row['time']
        
        flattened_row = {
            "date": date,
            "time": time
        }
        
        flattened_row['weather_status']= row['result']['weather_status']
        flattened_row['weather_temp']= row['result']['weather_temp']
        flattened_row['weather_humidity']= row['result']['weather_humidity']
        
        Weather_rows.append(flattened_row)



    Aircon_Normalize_Data = pd.DataFrame(Aircon_rows)
    Sensors_Normalize_Data = pd.DataFrame(Sensors_rows)
    Sensors_Normalize_Data_1 = pd.DataFrame(Sensors_rows_1)
    Weather_Normalize_Data = pd.DataFrame(Weather_rows)
    # For Aircon_Normalize_Data
    Aircon_Normalize_Data['datetime_str'] = Aircon_Normalize_Data['date'].astype(str) + ' ' + Aircon_Normalize_Data['time']
    Aircon_Normalize_Data['datetime'] = Aircon_Normalize_Data['datetime_str'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %I:%M:%S %p"))
    Aircon_Normalize_Data['timestamp'] = Aircon_Normalize_Data['datetime'].apply(lambda x: int(x.timestamp()))

    # For Sensors_Normalize_Data
    Sensors_Normalize_Data['datetime_str'] = Sensors_Normalize_Data['date'].astype(str) + ' ' + Sensors_Normalize_Data['time']
    Sensors_Normalize_Data['datetime'] = Sensors_Normalize_Data['datetime_str'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %I:%M:%S %p"))
    Sensors_Normalize_Data['timestamp'] = Sensors_Normalize_Data['datetime'].apply(lambda x: int(x.timestamp()))

    # For Sensors_Normalize_Data
    Sensors_Normalize_Data_1['datetime_str'] = Sensors_Normalize_Data_1['date'].astype(str) + ' ' + Sensors_Normalize_Data_1['time']
    Sensors_Normalize_Data_1['datetime'] = Sensors_Normalize_Data_1['datetime_str'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %I:%M:%S %p"))
    Sensors_Normalize_Data_1['timestamp'] = Sensors_Normalize_Data_1['datetime'].apply(lambda x: int(x.timestamp()))

    # For Weather_Normalize_Data
    Weather_Normalize_Data['datetime_str'] = Weather_Normalize_Data['date'].astype(str) + ' ' + Weather_Normalize_Data['time']
    Weather_Normalize_Data['datetime'] = Weather_Normalize_Data['datetime_str'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %I:%M:%S %p"))
    Weather_Normalize_Data['timestamp'] = Weather_Normalize_Data['datetime'].apply(lambda x: int(x.timestamp()))

    merged_data = pd.merge_asof(
        Aircon_Normalize_Data,  # Left DataFrame
        Sensors_Normalize_Data,      # Right DataFrame
        on='timestamp',   # Key column
        direction='nearest'    # Match the nearest time
    )

    merged_data = pd.merge_asof(
        merged_data,  # Left DataFrame
        Sensors_Normalize_Data_1,      # Right DataFrame
        on='timestamp',   # Key column
        direction='nearest'    # Match the nearest time
    )
    #Since weather is inaccurate i will just not include weather in data analysis
    # merged_data = pd.merge_asof(
    #     merged_data,  # Left DataFrame
    #     Weather_Normalize_Data,      # Right DataFrame
    #     on='timestamp',   # Key column
    #     direction='nearest'    # Match the nearest time
    # )

    temperature_col = [
        col for col in merged_data.columns 
        if "temperature" in col.lower()
    ]
    humidity_col = [
        col for col in merged_data.columns 
        if "humidity" in col.lower()
    ]
    c02_col = [
        col for col in merged_data.columns
        if "co2" in col.lower()
    ]

    outdoor_col = [
        col for col in merged_data.columns 
        if "24e124136d336145" in col.lower()
    ]

    occupancy_col_total_in = [
        col for col in merged_data.columns
        if "total_in" in col.lower()
    ]

    occupancy_col_total_out = [
        col for col in merged_data.columns
        if "total_out" in col.lower()
    ]

    def get_unit_columns(unit_number, columns):
        return [col for col in columns if f"FC_Unit_{unit_number}" in col]

    aircon_units = len([
        col for col in merged_data.columns
        if "FC_Unit_" in col and "_Status" in col and "Fan" not in col
    ])

    aircon_units_cols = {}

    for unit in range(1, aircon_units + 1):
        aircon_units_cols[f'Unit_{unit}'] = get_unit_columns(unit, merged_data.columns)


    final_data = pd.DataFrame()
    final_data["timestamp"] = merged_data["timestamp"]

    final_data["temperature"] = merged_data[temperature_col].apply(lambda x: round(x.mean(), 3), axis=1)
    final_data["humidity"] = merged_data[humidity_col].apply(lambda x: round(x.mean(),3), axis=1)
    final_data["co2"] = merged_data[c02_col].apply(lambda x: round(x.mean(),3), axis=1)

    final_data['power_consumption'] = merged_data['Total_Power']
    final_data['energy_consumption'] = merged_data['Total_Energy']

    final_data["outdoor_temperature"] = merged_data[outdoor_col]['24E124136D336145_temperature'].ffill()
    final_data["outdoor_humidity"] = merged_data[outdoor_col]['24E124136D336145_humidity'].ffill()

    # final_data["weather_status"] = merged_data["weather_status"]
    # final_data["weather_temp"] = merged_data['weather_temp']
    # final_data["weather_humid"] = merged_data['weather_humidity']


    merged_data[occupancy_col_total_in] = merged_data[occupancy_col_total_in].fillna(method='bfill')
    merged_data[occupancy_col_total_out] = merged_data[occupancy_col_total_out].fillna(method='bfill')

    final_data['occupancy'] = (
        merged_data[occupancy_col_total_in].sum(axis=1) - merged_data[occupancy_col_total_out].sum(axis=1)
    )


    for unit, columns in aircon_units_cols.items():
        for column in columns:
            if 'set_point' in column:
                final_data[column] = merged_data[column].replace(0, pd.NA).ffill()
            else:
                final_data[column] = merged_data[column].replace("ERROR", pd.NA).ffill()

    final_data.dropna(inplace=True)
    print("final_data_created")


    def getFCData(data, row_index):
        settings = []
        for i in range(1, aircon_units + 1):
            settings.append(data[f"FC_Unit_{i}_Status"].iloc[row_index])
            settings.append(data[f"FC_Unit_{i}_Fan_Status"].iloc[row_index])
            settings.append(data[f"FC_Unit_{i}_Set_Point"].iloc[row_index])
            settings.append(data[f"FC_Unit_{i}_Operation_Mode"].iloc[row_index])
            
        return settings

    def is_same_settings(data, curr_row_index, next_row_index):   
        return True if (getFCData(data, curr_row_index) == getFCData(data, next_row_index)) else False


    def is_all_off(data, curr_row_index, check_for_off):
        for i in range(1, aircon_units + 1):
            if data[f"FC_Unit_{i}_Status"].iloc[curr_row_index] == "ON":
                return not check_for_off
            
        return check_for_off

    def is_within_time_range(data, curr_row_index, next_row_index):
        if abs(data["timestamp"].iloc[next_row_index] - data["timestamp"].iloc[curr_row_index]) < 1800:
            return True
        else:
            return False


    # Reset index after filtering
    final_data.reset_index(drop=True, inplace=True)
    # final_data.to_csv("test.csv", index=False)


    aircon_status_result = pd.DataFrame()
    total_final_rows = final_data.shape[0]
    Aircon_Normalize_Data = Aircon_Normalize_Data.drop(['date', 'time', 'datetime_str', 'datetime', 'timestamp'], axis=1)

    for i in range(total_final_rows - 1):
        if is_all_off(final_data, i ,True):
            continue
        
        curr_energy = final_data["energy_consumption"].iloc[i]
        next_energy = final_data["energy_consumption"].iloc[i + 1]
        
        curr_temperature = final_data["temperature"].iloc[i]
        curr_humidity = final_data["humidity"].iloc[i]
        curr_outdoor_temp = final_data["outdoor_temperature"].iloc[i]
        curr_outdoor_humid = final_data["outdoor_humidity"].iloc[i]

        curr_co2 = final_data["co2"].iloc[i]
        
        if is_same_settings(final_data, i , i + 1):   
            diff = next_energy - curr_energy
        else:
            continue
            
        temp_df = pd.DataFrame({
            'energy_consumption': [diff],
            'temperature': [curr_temperature],
            'humidity': [curr_humidity],
            'outdoor_temp':[curr_outdoor_temp],
            'outdoor_humid':[curr_outdoor_humid],
            "co2":[curr_co2]
        })
        for col in Aircon_Normalize_Data.columns:
            temp_df[col] = final_data[col].iloc[i]
        
        aircon_status_result = pd.concat([aircon_status_result, temp_df], ignore_index=True)
    aircon_status_result = aircon_status_result[(aircon_status_result["energy_consumption"] > 0 ) & (aircon_status_result["energy_consumption"] < 50)]
    aircon_status_result.to_csv("saved_data/k_means_clusters.csv")
    