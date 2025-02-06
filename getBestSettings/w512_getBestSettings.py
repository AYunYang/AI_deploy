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
    weight_energy=0.5
    weight_time=0.5
    aircon_status_result = pd.read_csv("saved_data/aircon_status_W512_getBestSettings.csv")

    No_of_aircon = len([
        col for col in aircon_status_result.columns
        if "FC_Unit_" in col and "_Status" in col and "Fan" not in col
    ])

    lowest_timetaken = aircon_status_result["total_time_maintained"].min()
    highest_timetaken = aircon_status_result["total_time_maintained"].max()
    lowest_energy = aircon_status_result["total_energy_consumption"].min()
    highest_energy = aircon_status_result["total_energy_consumption"].max()

    aircon_status_result["total_time_maintained"] = (aircon_status_result["total_time_maintained"] - lowest_timetaken)/(highest_timetaken - lowest_timetaken)
    aircon_status_result["total_energy_consumption"] = (aircon_status_result["total_energy_consumption"] - lowest_energy)/(highest_energy - lowest_energy)

    aircon_status_result.to_csv("del ltrrrrrrr.csv")
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

        print(No_of_aircon)

        #if the provided temp is in range, it needs to check whether its value exist, else it will take the closest temperature
        if maintain_temp in aircon_status_result["current_temp"].values:
            closest_temp = maintain_temp
        else:
            closest_temp = getClosestTempToMaintain(maintain_temp)

        filtered_data = aircon_status_result[aircon_status_result["current_temp"] == closest_temp]

        # Sort by most time maintained (descending) and least energy consumption (ascending) not using enegy efficiency for now
        # filtered_data["efficiency_score"] = (filtered_data["total_energy_consumption"] * weight_energy) - (filtered_data["total_time_maintained"] * weight_time)
        # best_entries = filtered_data.loc[filtered_data["efficiency_score"].idxmin()]


        best_entries = filtered_data.sort_values(
            by=[ "total_energy_consumption","total_time_maintained"], ascending=[True,False]
        ).iloc[0]

        formatted_results = [
            {
                "others": {
                    "current_temperature": float(best_entries["current_temp"]),
                    "current_humidity": float(best_entries["current_humi"]), 
                    "total_time_maintained": float(best_entries["total_time_maintained"]), 
                    "total_energy_consumption": float(best_entries["total_energy_consumption"]),  
                    "energy_efficiency": float(best_entries["energy_efficiency"])
                }
            },
            {
                "results": [
                    {
                        "time_taken_seconds": None,  # or calculate if necessary
                        "aircon_settings": [
                            {
                                "unit": idx + 1,
                                "status": best_entries[f"FC_Unit_{idx+1}_Status"],
                                "fan_status": best_entries[f"FC_Unit_{idx+1}_Fan_Status"],
                                "set_point": best_entries[f"FC_Unit_{idx+1}_Set_Point"],
                                "operation_mode": best_entries[f"FC_Unit_{idx+1}_Operation_Mode"]
                            }
                            for idx in range(No_of_aircon)  # Adjust range according to the number of units
                        ]
                    }
                ]
            }
        ]


        return formatted_results
    
    result =  get_best_settings(temp)
    return result


