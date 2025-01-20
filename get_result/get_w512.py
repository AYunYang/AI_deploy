import json
import pandas as pd
import numpy as np

def get_w512(current_temp, target_temp):
    acceptable_range = 0.7
    key_to_find = f"Dictionary for target temp: {target_temp}"
    # temperature = np.arange(0,1.1,0.1)

    #To Test, just change the CSV and Json Files
    with open('saved_data/stored_dictionary_GBFS.json', 'r') as f:
        stored_dictionary = json.load(f) 

    print(type(stored_dictionary))
    aircon_status_result = pd.read_csv("saved_data/aircon_status_W512_GBFS.csv")

    # unique_keys = [key for key in stored_dictionary.keys() if key.startswith("Dictionary for target temp")]
    # print(unique_keys)

    # fan_status_columns = [col for col in aircon_status_result.columns if 'Fan_Status' in col]
    # unique_statuses = set()
    # for col in fan_status_columns:
    #     unique_statuses.update(aircon_status_result[col].dropna().unique())
    # fan_status_filitered = {status for status in unique_statuses if status != 'OFF'}
       
    aircon_units = len([
        col for col in aircon_status_result.columns
        if "FC_Unit_" in col and "_Status" in col and "Fan" not in col
    ])

    while key_to_find not in stored_dictionary:
        if (target_temp - current_temp) < 0:
            target_temp += 0.5
        else:
            target_temp -= 0.5
            
        key_to_find = f"Dictionary for target temp: {target_temp}"


    def expandPath(row_index):
        unit_data = []
        for i in range(1, aircon_units + 1):
            unit_info = {
                "unit": i,
                "status": aircon_status_result[f"FC_Unit_{i}_Status"].iloc[row_index],
                "fan_status": aircon_status_result[f"FC_Unit_{i}_Fan_Status"].iloc[row_index],
                "set_point": aircon_status_result[f"FC_Unit_{i}_Set_Point"].iloc[row_index],
                "operation_mode": aircon_status_result[f"FC_Unit_{i}_Operation_Mode"].iloc[row_index],
            }
            unit_data.append(unit_info)
        return unit_data
     
    # def expandpath_withrandomness(row_index):
    #     unit_data = []
    #     randomness = np.round(np.random.choice(temperature, 1)[0], 1)  
    #     scaled_change = randomness * 0.5

    #     print(f"randomness: {randomness}, scaled_change: {scaled_change}")

    #     if randomness <= 0.3:
    #         unit_data = expandPath(row_index)
    #         print()

    #     elif 0.3 < randomness <= 0.6:
    #         unit_to_change = np.random.randint(1, aircon_units + 1)
    #         print(f"Unit to change: {unit_to_change}")
            
    #         current_set_point = aircon_status_result[f"FC_Unit_{unit_to_change}_Set_Point"].iloc[row_index]
    #         new_set_point = current_set_point + np.random.choice([-1, 1]) * scaled_change
    #         print(f"{current_set_point} changed to {new_set_point}")
            
    #         unit_data = expandPath(row_index) 
    #         unit_data[unit_to_change - 1]["set_point"] = new_set_point
    #         print()
    #     else:
    #         unit_to_change = np.random.randint(1, aircon_units + 1)
    #         print(f"Unit to change: {unit_to_change}")

    #         current_fan_status = aircon_status_result[f"FC_Unit_{unit_to_change}_Fan_Status"].iloc[row_index]
    #         current_set_point = aircon_status_result[f"FC_Unit_{unit_to_change}_Set_Point"].iloc[row_index]
    #         new_set_point = current_set_point + np.random.choice([-1, 1]) * scaled_change
    #         print(f"{current_set_point} changed to {new_set_point}")

                    
    #         fan_mode_to_change = np.random.choice(list(fan_status_filitered))

    #         while current_fan_status == fan_mode_to_change:
    #             fan_mode_to_change = np.random.choice(list(fan_status_filitered))
    #             print("was same")

    #         print(f"{current_fan_status} changed to {fan_mode_to_change}")
            
    #         unit_data = expandPath(row_index)  # Get the base unit data
    #         unit_data[unit_to_change - 1]["set_point"] = new_set_point  # Update the set point
    #         unit_data[unit_to_change - 1]["fan_status"] = fan_mode_to_change  # Update the fan mode
    #         print()
    #     return unit_data   
        
    def findClosestTemperature(current_temp, paths):
        """Find the index of the closest available temperature in the paths dictionary."""
        closest_temp_index = None
        smallest_difference = float('inf')
        
        for key, value in paths.items():
            if isinstance(value, dict) and 'starting_temp' in value:
                if value['starting_temp'] and value['path']:  #ensure that paths[index] has both values in 'starting_temp' and 'path'
                    difference = abs(value['starting_temp'] - current_temp)
                    if difference < smallest_difference:
                        smallest_difference = difference
                        closest_temp_index = key   
        return closest_temp_index



    # Check if either key exists in stored_dictionary
    if key_to_find in stored_dictionary:
        print(f"Using {key_to_find}")
        stored_dict_key = stored_dictionary[key_to_find]

        filtered_paths = {
            key: value for key, value in stored_dict_key.items()
            if isinstance(value, dict) and 'starting_temp' in value
            and abs(value['starting_temp'] - current_temp) < acceptable_range
            and value["path"]
        }
        aircon_settings_result = []


        if filtered_paths:
            # Find the path with the smallest factor
            smallest_factor_path = min(filtered_paths.keys(), key=lambda x: filtered_paths[x]['factor'])
            aircon_settings_result.append({'path': str(stored_dict_key[smallest_factor_path])})
            final_results = []


            for index, value in enumerate(stored_dict_key[smallest_factor_path]['path']):
                result = expandPath(value)
                timetaken_Seconds = int(stored_dict_key[smallest_factor_path]['time_taken'][index])
                final_results.append({
                    'time_taken_seconds': timetaken_Seconds,
                    'aircon_settings': result
                })
            aircon_settings_result.append({'results': final_results})

            return aircon_settings_result
        else:
            print("No paths found within the acceptable range of current temp.")
            print("Finding the closest temperature in the algorithm.")


            closest_temp_index = findClosestTemperature(current_temp, stored_dict_key)
            
            if closest_temp_index is not None:
                print("Closest temperature found at index:", closest_temp_index , "\n")

                aircon_settings_result.append({'path': str(stored_dict_key[closest_temp_index])})

                final_results =[]
                
                for index, value in enumerate(stored_dict_key[closest_temp_index]['path']):
                    result = expandPath(value)
                    timetaken_Seconds = int(stored_dict_key[closest_temp_index]['time_taken'][index])
                    final_results.append({
                        'time_taken_seconds': timetaken_Seconds,
                        'aircon_settings': result
                    })
                aircon_settings_result.append({'results': final_results})
                return aircon_settings_result
            else:
                print("No valid paths available, even for the closest temperature.")
                return "No valid paths available, even for the closest temperature."

    else:
        print(f"{key_to_find} does not exist")
        return f"{key_to_find} does not exist"


    

