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


def execute_w512_getBestSettings(temp):


    Sensors_Data = pd.read_json('database_data/W512_readings.json')
    Aircon_Data = pd.read_json('database_data/W512_aircon_status.json')
    Weather_Data = pd.read_json('database_data/Weather_data.json')

    # Aircon_Data = Aircon_Data.iloc[3194:]
    # Normalize the data
    Aircon_rows = []

    for _, row in Aircon_Data.iterrows():
        date = row['date']
        time = row['time']
        
        flattened_row = {
            "date": date,
            "time": time
        }
        
        fc_readings = row['FC_FullStatus_Readings']
        
        if fc_readings is not None and isinstance(fc_readings, dict):
            for unit, data in fc_readings.items():
                if any(data.get("Set_Point", None) == 404.0 or data.get("Set_Point", None) < 0 for data in fc_readings.values()):
                    continue
                flattened_row[f"{unit}_Status"] = data.get("Status", None)
                flattened_row[f"{unit}_Fan_Status"] = data.get("Fan_Status", None)
                flattened_row[f"{unit}_Set_Point"] = data.get("Set_Point", None)
                flattened_row[f"{unit}_Operation_Mode"] = data.get("Operation_Mode", None)
            
        
        Aircon_rows.append(flattened_row)

    Sensors_rows = []
    include_keys_1 = ["24E124725E285123", "24E124725E331695","24E124725E331744",
                        "24E124725E332483","24E124725E290348","24E124725E331733","24E124725E286745"]#"24E124136D316361" is suppiosed to be outdoor but it is not outdoor yet
    include_keys_2 = ["Sensor_1","Sensor_3","Sensor_6"]
    for _, row in Sensors_Data.iterrows():
        
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

        if energy_readings is not None and isinstance(energy_readings, dict):
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
    Weather_Normalize_Data = pd.DataFrame(Weather_rows)
    # For Aircon_Normalize_Data
    Aircon_Normalize_Data['datetime_str'] = Aircon_Normalize_Data['date'].astype(str) + ' ' + Aircon_Normalize_Data['time']
    Aircon_Normalize_Data['datetime'] = Aircon_Normalize_Data['datetime_str'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %I:%M:%S %p"))
    Aircon_Normalize_Data['timestamp'] = Aircon_Normalize_Data['datetime'].apply(lambda x: int(x.timestamp()))

    # For Sensors_Normalize_Data
    Sensors_Normalize_Data['datetime_str'] = Sensors_Normalize_Data['date'].astype(str) + ' ' + Sensors_Normalize_Data['time']
    Sensors_Normalize_Data['datetime'] = Sensors_Normalize_Data['datetime_str'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %I:%M:%S %p"))
    Sensors_Normalize_Data['timestamp'] = Sensors_Normalize_Data['datetime'].apply(lambda x: int(x.timestamp()))

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
        Weather_Normalize_Data,      # Right DataFrame
        on='timestamp',   # Key column
        direction='nearest'    # Match the nearest time
    )

    temperature_col = [
        col for col in merged_data.columns 
        if "temperature" in col.lower()
    ]
    humidity_col = [
        col for col in merged_data.columns 
        if "humidity" in col.lower()
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

    final_data['power_consumption'] = merged_data['Total_Power']
    final_data['energy_consumption'] = merged_data['Total_Energy']

    for unit, columns in aircon_units_cols.items():
        for column in columns:
            if 'set_point' in column:
                final_data[column] = merged_data[column].replace(0, pd.NA).ffill()
            else:
                final_data[column] = merged_data[column].replace("ERROR", pd.NA).ffill()

    final_data.dropna(inplace=True)
    # final_data.reset_index(drop=True, inplace=True)#######################################


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

    def is_within_temperature_range(current_temp, next_temp):
        range_factor = 0.5
        if current_temp - range_factor <= next_temp <= current_temp + range_factor:
            return True
        return False


    # final_data.to_csv('final_data_W512_getbestsett.csv', index=False)


    aircon_status_result = pd.DataFrame()
    total_final_rows = final_data.shape[0]
    Aircon_Normalize_Data = Aircon_Normalize_Data.drop(['date', 'time', 'datetime_str', 'datetime', 'timestamp'], axis=1)


    for i in range(total_final_rows - 1):
        if is_all_off(final_data, i, True):
            continue
        
        rows = []
        # time_taken = []
        # energy_consumption = []
        # previous_temp = []
        # previous_humi = []
        
        curr_timestamp = final_data["timestamp"].iloc[i]
        curr_energy = final_data["energy_consumption"].iloc[i]
        curr_temperature = final_data["temperature"].iloc[i]
        curr_humidity = final_data["humidity"].iloc[i]

        total_time_maintained = 0
        total_energy_consumption = 0

        
        while i < total_final_rows and is_same_settings(final_data, i + 1, i) and is_within_temperature_range(curr_temperature,final_data["temperature"].iloc[i + 1]):
            timetaken =  final_data["timestamp"].iloc[i + 1] - curr_timestamp
            energyconsum = final_data["energy_consumption"].iloc[i + 1] - curr_energy 

            if timetaken < 15 or timetaken > 3600:
                break
            if energyconsum < 0:
                break

            rows.append(i + 1)

            total_time_maintained += timetaken
            total_energy_consumption += energyconsum
            
            i += 1

            

        if total_time_maintained == 0 or total_energy_consumption == 0:
            continue
            
        temp_df = pd.DataFrame({
                'current_temp': [curr_temperature],
                'current_humi': [curr_humidity],
                'total_time_maintained':total_time_maintained,
                'total_energy_consumption': total_energy_consumption,
                'energy_efficiency': total_time_maintained / total_energy_consumption
            })
        for col in Aircon_Normalize_Data.columns:
            temp_df[col] = final_data[col].iloc[i]
        
            
        aircon_status_result = pd.concat([aircon_status_result, temp_df], ignore_index=False)

            

            
            
            
    print("Finished")
    # aircon_status_result = aircon_status_result[aircon_status_result['total_time_maintained'] != 0 ]
    aircon_status_result = aircon_status_result.sort_values(by=['current_temp'], ascending=False)
    # aircon_status_result.to_csv('aircon_status_W512_getbestsett.csv', index=False)
    # aircon_status_result.info()

           
    def getClosestTempToMaintain(maintain_temp):
        closest_temp_index = None
        smallest_difference = float('inf')

        # Iterate through the current_temp column to find the closest value
        for index, value in enumerate(aircon_status_result["current_temp"]):
            difference = abs(value - maintain_temp)
            if difference < smallest_difference:
                smallest_difference = difference
                closest_temp_index = index

        # Retrieve the closest temperature value using .iloc
        closest_temp_value = aircon_status_result["current_temp"].iloc[closest_temp_index]
        return closest_temp_value




    def get_best_settings(maintain_temp):
        best_entries = {}

        # #if provided temperature is out of range , it will ignore
        # if maintain_temp < min(aircon_status_result["current_temp"].values) - 0.5 or maintain_temp > max(aircon_status_result["current_temp"].values) + 0.5:
        #     print(f"Maintain temperature {maintain_temp} is out of range.")
        #     return None

        #if the provided temp is in range, it needs to check whether its value exist, else it will take the closest temperature
        if maintain_temp in aircon_status_result["current_temp"].values:
            closest_temp = maintain_temp
        else:
            closest_temp = getClosestTempToMaintain(maintain_temp)

        filtered_data = aircon_status_result[aircon_status_result["current_temp"] == closest_temp]

        # Sort by most time maintained (descending) and least energy consumption (ascending)
        best_entries = filtered_data.sort_values(
            by=[ "energy_efficiency", "total_energy_consumption","total_time_maintained"], ascending=[False,False, True]
        ).iloc[0]


        return best_entries
    
    result =  get_best_settings(temp).to_dict()
    return result


