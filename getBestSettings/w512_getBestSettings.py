#imports
import pandas as pd
import numpy as np

def execute_w512_getBestSettings(temp):
    weight_energy = 0.5
    weight_time = 0.5
    
    try:
        aircon_status_result = pd.read_csv("saved_data/aircon_status_W512_getBestSettings.csv")
    except FileNotFoundError:
        print("Error: CSV file not found.")
        return None

    No_of_aircon = len([
        col for col in aircon_status_result.columns
        if "FC_Unit_" in col and "_Status" in col and "Fan" not in col
    ])

    # Prevent division by zero
    epsilon = 1e-6  
    lowest_timetaken, highest_timetaken = aircon_status_result["total_time_maintained"].min(), aircon_status_result["total_time_maintained"].max()
    lowest_energy, highest_energy = aircon_status_result["total_energy_consumption"].min(), aircon_status_result["total_energy_consumption"].max()

    aircon_status_result["total_time_maintained"] = (aircon_status_result["total_time_maintained"] - lowest_timetaken) / (highest_timetaken - lowest_timetaken + epsilon)
    aircon_status_result["total_energy_consumption"] = (aircon_status_result["total_energy_consumption"] - lowest_energy) / (highest_energy - lowest_energy + epsilon)

    def getClosestTempToMaintain(maintain_temp):
        if aircon_status_result.empty:
            return None
        # Vectorized approach for efficiency
        closest_temp_index = np.argmin(np.abs(aircon_status_result["current_temp"] - maintain_temp))
        return aircon_status_result["current_temp"].iloc[closest_temp_index]

    def get_best_settings(maintain_temp):
        if aircon_status_result.empty:
            return None
        
        if maintain_temp in aircon_status_result["current_temp"].values:
            closest_temp = maintain_temp
        else:
            closest_temp = getClosestTempToMaintain(maintain_temp)
            if closest_temp is None:
                print("No valid temperature data found.")
                return None

        filtered_data = aircon_status_result[aircon_status_result["current_temp"] == closest_temp]

        # Compute efficiency score
        filtered_data["efficiency_score"] = (filtered_data["total_energy_consumption"] * weight_energy) + (filtered_data["total_time_maintained"] * weight_time)
        best_entries = filtered_data.nsmallest(1, "efficiency_score").iloc[0]

        formatted_results = [
            {
                "others": {
                    "current_temperature": float(best_entries["current_temp"]),
                    "current_humidity": float(best_entries["current_humi"]), 
                    "total_time_maintained": float(best_entries["total_time_maintained"]), 
                    "total_energy_consumption": float(best_entries["total_energy_consumption"]),  
                    "efficiency_score": float(best_entries["efficiency_score"])
                }
            },
            {
                "results": [
                    {
                        "time_taken_seconds": None,  
                        "aircon_settings": [
                            {
                                "unit": idx + 1,
                                "status": best_entries[f"FC_Unit_{idx+1}_Status"],
                                "fan_status": best_entries[f"FC_Unit_{idx+1}_Fan_Status"],
                                "set_point": best_entries[f"FC_Unit_{idx+1}_Set_Point"],
                                "operation_mode": best_entries[f"FC_Unit_{idx+1}_Operation_Mode"]
                            }
                            for idx in range(No_of_aircon)
                        ]
                    }
                ]
            }
        ]
        return formatted_results

    return get_best_settings(temp)
