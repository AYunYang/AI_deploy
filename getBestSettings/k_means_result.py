import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import pairwise_distances_argmin_min

def get_kmeans_result(temp,humid,outdoortemp,outdoorhumid,co2):
    df = pd.read_csv("saved_data/k_means_clusters.csv")
    # Select features for clustering
    features = [
        # 'energy_consumption', 
        'temperature', 
        'humidity', 
        'outdoor_temp', 
        'outdoor_humid',
        'co2'
    ]
    # Prepare the data
    X = df[features]

    # Standardize the features
    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(X)

    # Perform K-means clustering
    kmeans = KMeans(n_clusters=20, random_state=42)
    df['cluster'] = kmeans.fit_predict(X_scaled)

    # Assume we have a new input
    new_input_df = pd.DataFrame([[temp, humid, outdoortemp, outdoorhumid, co2]], columns=features)
    centroids = kmeans.cluster_centers_
    new_input_scaled = scaler.transform(new_input_df)
    distances = pairwise_distances_argmin_min(new_input_scaled, centroids)

    # distances[0] is the index of the closest cluster
    closest_cluster_index = distances[0]

    filter_result = df[df["cluster"] == closest_cluster_index[0]]


    fc_columns = [col for col in filter_result.columns if "FC_Unit" in col]

    grouped = filter_result.groupby(fc_columns)["energy_consumption"].mean().reset_index()
    best_settings = grouped.sort_values(by="energy_consumption", ascending=True)
    best_settings = best_settings.iloc[0]

    number_of_fc_unit = len([col for col in grouped.columns if "FC_Unit_" in col and "_Status" in col and "Fan" not in col])

    # Create a dictionary to format the result
    formatted_result = [
            {
                "others": {
                    "energy_consumption": best_settings["energy_consumption"]
                }
            },
            {
                "results": [
                    {
                        "time_taken_seconds": None,  # or calculate if necessary
                        "aircon_settings": [
                            {
                                "unit": idx + 1,
                                "status": best_settings[f"FC_Unit_{idx+1}_Status"],
                                "fan_status": best_settings[f"FC_Unit_{idx+1}_Fan_Status"],
                                "set_point": best_settings[f"FC_Unit_{idx+1}_Set_Point"],
                                "operation_mode": best_settings[f"FC_Unit_{idx+1}_Operation_Mode"]
                            }
                            for idx in range(number_of_fc_unit)  # Adjust range according to the number of units
                        ]
                    }
                ]
            }]

    return formatted_result