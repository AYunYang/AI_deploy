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

def execute_Astar():
    print("Algorihtm: Astar")
    start_time = clock.time()

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
                        "24E124725E332483","24E124725E290348","24E124725E331733","24E124725E286745","24E124725E332564"]#"24E124136D316361" is suppiosed to be outdoor but it is not outdoor yet
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
        
    print("Normalizing data")



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
    final_data["weather_status"] = merged_data["weather_status"]
    for unit, columns in aircon_units_cols.items():
        for column in columns:
            if 'set_point' in column:
                final_data[column] = merged_data[column].replace(0, pd.NA).ffill()
            else:
                final_data[column] = merged_data[column].replace("ERROR", pd.NA).ffill()

    final_data.dropna(inplace=True)
    # final_data.reset_index(drop=True, inplace=True)#######################################


    # final_data.to_csv('final_data_W512_Astar.csv', index=False)

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
        range_factor = 0.3
        if current_temp - range_factor <= next_temp <= current_temp + range_factor:
            return True
        return False



    aircon_status_result = pd.DataFrame()
    aircon_status_getBestSettings_result = pd.DataFrame()
    total_final_rows = final_data.shape[0]
    Aircon_Normalize_Data = Aircon_Normalize_Data.drop(['date', 'time', 'datetime_str', 'datetime', 'timestamp'], axis=1)

    for i in range(total_final_rows - 1):
        if is_all_off(final_data, i, True):
            continue

        rows = []
        time_taken = []
        energy_consumption = []
        future_temp = []
        future_humi = []
        
        curr_timestamp = final_data["timestamp"].iloc[i]
        curr_energy = final_data["energy_consumption"].iloc[i]
        curr_temperature = final_data["temperature"].iloc[i]
        curr_humidity = final_data["humidity"].iloc[i]

        
        while i < total_final_rows - 1 and is_same_settings(final_data, i + 1, i):
            timetaken = final_data["timestamp"].iloc[i + 1] - curr_timestamp 
            energyConsumption = final_data["energy_consumption"].iloc[i + 1] - curr_energy
            if timetaken < 600 or timetaken > 4200:
                break
            if energyConsumption <= 0:
                break
            rows.append(i - 1)
            time_taken.append(timetaken)
            energy_consumption.append(energyConsumption)
            future_temp.append(final_data["temperature"].iloc[i + 1])
            future_humi.append(final_data["humidity"].iloc[i + 1])
            
            i += 1
            
        temp_df = pd.DataFrame({
                'timestamp': [curr_timestamp],
                'rows': [rows],
                'time_taken': [time_taken],
                'energy_consumption': [energy_consumption],
                'future_temp': [future_temp],
                'future_humi': [future_humi],
                'current_temp': [curr_temperature],
                'current_humi': [curr_humidity],
                'weather_status':[final_data["weather_status"].iloc[i]]
            })
        for col in Aircon_Normalize_Data.columns:
            temp_df[col] = final_data[col].iloc[i]
        
            
        aircon_status_result = pd.concat([aircon_status_result, temp_df], ignore_index=False)        
    print("Finished 1")

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

        
        while i < total_final_rows - 1 and is_same_settings(final_data, i + 1, i) and is_within_temperature_range(curr_temperature,final_data["temperature"].iloc[i + 1]):
            timetaken =  final_data["timestamp"].iloc[i + 1] - curr_timestamp
            energyconsum = final_data["energy_consumption"].iloc[i + 1] - curr_energy 

            if timetaken < 1200:
                break
            if energyconsum <= 0:
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
                'energy_efficiency': total_time_maintained / total_energy_consumption # This value is not going to be used later in getBestSettings
            })
        for col in Aircon_Normalize_Data.columns:
            temp_df[col] = final_data[col].iloc[i]
        
            
        aircon_status_getBestSettings_result = pd.concat([aircon_status_getBestSettings_result, temp_df], ignore_index=False)
    print("Finished 2")

    aircon_status_result = aircon_status_result[
        (aircon_status_result['weather_status'] != 'Rain') & 
        (aircon_status_result['weather_status'] != 'Thunderstorm')
    ]
    print("weather")

    aircon_status_result = aircon_status_result.sort_values(by=['current_temp'], ascending=False)
    aircon_status_result.to_csv('saved_data/aircon_status_W512_AStar.csv', index=False)
    # aircon_status_result.info()
    aircon_status_getBestSettings_result = aircon_status_getBestSettings_result.sort_values(by=['current_temp'], ascending=False)
    aircon_status_getBestSettings_result.to_csv("saved_data/aircon_status_W512_getBestSettings.csv", index=False)

    #############
    target_temp_range = np.arange(20, 29.5, 0.5)
    time_factor = 0.5
    energy_factor = 0.5
    acceptable_range = 0.5
    total_rows = aircon_status_result.shape[0]
    stored_dictionary = {}

    ###########norm data
    highest_timetaken = 0
    highest_energy = 0
    lowest_timetaken = float('inf')
    lowest_energy = float('inf')

    def normalize_time(current_value):
        return (current_value - lowest_timetaken)/(highest_timetaken - lowest_timetaken)
        
    def normalize_energy(current_value):
        return (current_value - lowest_energy)/(highest_energy - lowest_energy)

        
    for index, row in aircon_status_result.iterrows():
        if not row["time_taken"] or not row["energy_consumption"]:
            continue
        
        if max(row["time_taken"]) > highest_timetaken:
            highest_timetaken = max(row["time_taken"])
            
        if min(row["time_taken"]) < lowest_timetaken:
            lowest_timetaken = min(row["time_taken"])
            
        if max(row["energy_consumption"]) > highest_energy:
            highest_energy = max(row["energy_consumption"])

        if min(row["energy_consumption"]) < lowest_energy:
            lowest_energy = min(row["energy_consumption"])

    ##########


    def getRowData(row_index):
        temperature = aircon_status_result["current_temp"].iloc[row_index]
        humidity = aircon_status_result["current_humi"].iloc[row_index]
        
        return [temperature, humidity]

    def getArrayData(row_index, array_index):
        time_taken = aircon_status_result["time_taken"].iloc[row_index]
        energy_consumption = aircon_status_result["energy_consumption"].iloc[row_index]
        temperature = aircon_status_result["future_temp"].iloc[row_index]
        humidity = aircon_status_result["future_humi"].iloc[row_index]
        
        return [temperature[array_index], humidity[array_index], time_taken[array_index], energy_consumption[array_index]]

    def comparePath(best_path, current_path):    
        if best_path['factor'] > current_path['factor']:
            return True
        
        return False

    # Get heuristic score
    h_score = 0
    total_score = 0
    total_checked = 0
    for i in range(total_rows):
        row_data = getRowData(i)
        for j in range(len(aircon_status_result['rows'].iloc[i])):
            array_data = getArrayData(i, j)
            if (abs(row_data[0] - array_data[0]) > 0):
                total_score += ((normalize_energy(array_data[3]) * energy_factor) + 
                                (normalize_time(array_data[2]) * time_factor))/(abs(row_data[0] - array_data[0]))
                total_checked += 1
            
    h_score = total_score/total_checked
    print(h_score)

    # h_score is the average amount of energy and time is needed for 1 degree celius

    ###############

    def calculateScore(array_data):
        return (normalize_energy(array_data[3]) * energy_factor) + (normalize_time(array_data[2]) * time_factor)

    def calculateHeuristicScore(start_temp, end_temp):
        return abs(end_temp - start_temp) * h_score

    def findMinMax():
        first_temp = aircon_status_result["current_temp"].iloc[0]
        second_temp = aircon_status_result["current_temp"].iloc[total_rows - 1]
        
        ascending = False
        if second_temp > first_temp:
            ascending = True
        
        for i in range(len(aircon_status_result["future_temp"].iloc[total_rows - 1])):
            if ascending:
                second_temp = max(aircon_status_result["future_temp"].iloc[total_rows - 1][i], second_temp)
            else:
                second_temp = min(aircon_status_result["future_temp"].iloc[total_rows - 1][i], second_temp)
        
        return [first_temp, second_temp] if ascending else [second_temp, first_temp]

    for target_temp in target_temp_range:
        paths = {}

        
        def AStar(curr_row, curr_arr, checked_nodes):
            # print(curr_row, curr_arr)
            rowData = getRowData(curr_row)
            arrayData = getArrayData(curr_row, curr_arr)
            # Returns the heuristic score and the path
            
            if abs(arrayData[0] - target_temp) < acceptable_range:
                # Scenario 1
                curr_path = {
                    'energy_consumption': [arrayData[3]],
                    'time_taken': [arrayData[2]],
                    'factor': normalize_energy(array_data[3]) * energy_factor + normalize_time(array_data[2]) * time_factor,
                    'starting_temp': rowData[0],
                    'starting_humi': rowData[1],
                    'ending_temp': arrayData[0],
                    'ending_humi': arrayData[1],
                    'path': [curr_row] 
                }
                
                paths[curr_row] = curr_path
                return curr_path['factor'], curr_path
                    
            path = {}
            lowestHeuristicScore = float("inf")
            lowestScoreIndex = [-1, -1]
            rowData = getRowData(curr_row)
            
            # A -> B
            # Find B -> C
            # B can be B1, B2, B3, find the best B -> C
            while True:
                out_of_range = False
                for i in range(curr_row + 1, total_rows):
                    nextRowData = getRowData(i)
                    if checked_nodes[i][0]:
                        continue
                        
                    if abs(nextRowData[0] - arrayData[0]) > acceptable_range:
                        if out_of_range:
                            break
                        continue
                    
                    out_of_range = True
                    if i in paths:
                        # if i is already in the path
                        # that means path i is optimized and can be used 
                        heuristicScore = paths[i]['factor']
                        if heuristicScore < lowestHeuristicScore:
                            lowestHeuristicScore = heuristicScore
                            lowestScoreIndex = [i, -1]
                        continue
                        
                    for j in range(len(aircon_status_result['rows'].iloc[i])):
                        nextArrayData = getArrayData(i, j)
                        if checked_nodes[i][j + 1]:
                            continue
                        # find the node with the lowest h score
                        heuristicScore = calculateScore(nextArrayData) + calculateHeuristicScore(nextArrayData[0], target_temp)
                        if heuristicScore < lowestHeuristicScore:
                            lowestHeuristicScore = heuristicScore
                            lowestScoreIndex = [i, j]
                            
                if lowestScoreIndex[0] == -1:
                    break
                    
                if lowestScoreIndex[0] in paths:
                    # The shortest path is already found
                    # Scenario 1
                    if not paths[lowestScoreIndex[0]]["energy_consumption"]:
                        checked_nodes[i][0] = True
                        lowestHeuristicScore = float("inf")
                        lowestScoreIndex = [-1, -1]
                        continue
                        
                    path = paths[lowestScoreIndex[0]]
                    curr_path = {
                        'energy_consumption': [arrayData[3]] + path['energy_consumption'],
                        'time_taken': [arrayData[2]] + path['time_taken'],
                        'starting_temp': rowData[0],
                        'starting_humi': rowData[1],
                        'ending_temp': path['ending_temp'],
                        'ending_humi': path['ending_humi'],
                        'path': [curr_row] + path['path'] 
                    }
                    curr_path['factor'] = normalize_energy(sum(curr_path['energy_consumption'])) * energy_factor + normalize_time(sum(curr_path['time_taken'])) * time_factor

                    paths[curr_row] = curr_path
                    
                    return curr_path['factor'], curr_path
                
                if checked_nodes[lowestScoreIndex[0]][lowestScoreIndex[1] + 1]:
                    # Scenario 1: the next Node is Valid ( Reaches the target )
                    # Scenario 2: all Node in Array is Invalid ( Unable to reach the target )
                    # Check if node is valid/invalid
                    if lowestHeuristicScore == float("inf"):
                        # Scenario 2
                        paths[curr_row] = {
                            'energy_consumption': [], 
                            'starting_temp': 0, 
                            'starting_humi': 0, 
                            'time_taken': [], 
                            'factor': float('inf'), 
                            'path': []
                        }
                        
                        return float("inf"), paths[curr_row]
                    else:
                        # Scenario 1
                        lowestH = min(successful_path.keys())
                        best_path = successful_path[lowestH]
                        
                        curr_path = {
                            'energy_consumption': [arrayData[3]] + best_path['energy_consumption'],
                            'time_taken': [arrayData[2]] + best_path['time_taken'],
                            'starting_temp': rowData[0],
                            'starting_humi': rowData[1],
                            'ending_temp': best_path['ending_temp'],
                            'ending_humi': best_path['ending_humi'],
                            'path': [curr_row] + best_path['path'] 
                        }
                        
                        curr_path['factor'] = normalize_energy(sum(curr_path['energy_consumption'])) * energy_factor + normalize_time(sum(curr_path['time_taken'])) * time_factor

                        
                        paths[curr_row] = curr_path
                        
                    return curr_path['factor'], curr_path
                
                # print("Lowest Score Indexex: ", lowestScoreIndex[0], lowestScoreIndex[1])
                # If the path require finding
                checked_nodes[lowestScoreIndex[0]][lowestScoreIndex[1] + 1] = True
                # path returned from AStar is B -> C -> D
                tempScore, path = AStar(lowestScoreIndex[0], lowestScoreIndex[1], checked_nodes)
                lowestHeuristicScore = calculateScore(arrayData) + tempScore
                
                if tempScore != float("inf"):
                    successful_path[tempScore] = path
            
            paths[curr_row] = {
                'energy_consumption': [], 
                'starting_temp': 0, 
                'starting_humi': 0, 
                'time_taken': [], 
                'factor': float('inf'), 
                'path': []
            }

            return float('inf'), paths[curr_row]
                    
    #     minTemp, maxTemp = findMinMax()
    #     if target_temp < minTemp or target_temp > maxTemp:
    #         continue
        # start_temp = aircon_status_result["current_temp"].iloc[0]
        for i in range(total_rows):
            if i not in paths:
                # start A* algorithm for the array
                path = []
                lowestHeuristicScore = float("inf")
                lowestScoreIndex = -1
                rowData = getRowData(i)
                checked_nodes = [False] * len(aircon_status_result['rows'].iloc[i])
                successful_path = {}
                while True:
                    for j in range(len(aircon_status_result['rows'].iloc[i])):
                        if checked_nodes[j]:
                            # Skip nodes that are already checked
                            continue

                        arrayData = getArrayData(i, j)
                        # find the node with the lowest h score
                        heuristicScore = calculateScore(arrayData) + calculateHeuristicScore(arrayData[0], target_temp)
                        if heuristicScore < lowestHeuristicScore:
                            lowestHeuristicScore = heuristicScore
                            lowestScoreIndex = j
                    
                    # print("Current Lowest Index: ", lowestScoreIndex)
                    if lowestScoreIndex == -1 or checked_nodes[lowestScoreIndex]:
                        break

                    arrayData = getArrayData(i, lowestScoreIndex)
                    checked_nodes[lowestScoreIndex] = True
                    # path returned from AStar is B -> C -> D
                    temp_checked_nodes = [[False] * (len(aircon_status_result['rows'].iloc[i]) + 1) for i in range(total_rows)]
                    temp_checked_nodes[i][lowestScoreIndex + 1] = True
                    tempScore, path = AStar(i, lowestScoreIndex, temp_checked_nodes)
                    lowestHeuristicScore = calculateScore(arrayData) + tempScore              
                
        if not any(path['factor'] < float('inf') for path in paths.values()):
            print(f"Target temp {target_temp} not achievable.")
            continue  # Move to the next target temperature
        else:
            stored_dictionary[f"Dictionary for target temp: {target_temp}"] = paths.copy()
            print(f"Dictionary for target temp: {target_temp} -> avaliable" )
            
    end_time = clock.time()
    elapsed_time = end_time - start_time
    elapsed_time_minutes = elapsed_time / 60
    print(f"Processing time: {elapsed_time_minutes:.2f} minutes")

    with open('saved_data/stored_dictionary_Astar.json', 'w') as f:
        json.dump(stored_dictionary, f, default=str, indent=4)

    print("successful json conversion")
    print(f"data Preparation and stored dictionary successful. Time taken (Astar): {elapsed_time_minutes:.2f} minutes with {acceptable_range} acceptable range")

    return f"data Preparation and stored dictionary successful. Time taken: {elapsed_time_minutes:.2f} minutes"
