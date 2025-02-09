from fastapi import FastAPI, HTTPException, BackgroundTasks
from concurrent.futures import ThreadPoolExecutor
from get_result.get_w512 import get_w512
from get_result.get_spgg import get_spgg
from getBestSettings.w512_getBestSettings import execute_w512_getBestSettings
from getBestSettings.spgg_getBestSettings import execute_spgg_getBestSettings
from getBestSettings.k_means_result import get_kmeans_result
from test_algorithms.GBFS import execute_GBFS
from test_algorithms.Astar import execute_Astar
from test_algorithms.backtracking import execute_backtracking
from test_algorithms.backtracking_SPGG import execute_backtracking_spgg
from test_algorithms.k_means import execute_kmeans
from mongodb_data import fetch_data
app = FastAPI()

# Create a ThreadPoolExecutor for running background tasks
executor = ThreadPoolExecutor()

# A wrapper function to run the training process
def execute_training(site: str , algorithm: str):


    if site.lower() == 'w512':
        try:
            fetch_data() #fetch data to update to latest data
            print("Fetched data")
        except:
            print("failed to fetch data")

        if algorithm.lower() == 'astar':
            result = execute_Astar()
            print("Astar Training completed")

        elif algorithm.lower() == 'gbfs':
            result = execute_GBFS()
            print("GBFS Training completed")

        elif algorithm.lower() == 'backtracking':
            result = execute_backtracking()
            print("backtracking Training completed")

        elif algorithm.lower() == 'kmeans':
            result = execute_kmeans()
            print("k-means Training completed")

        elif algorithm.lower() == 'all':
            execute_backtracking()
            print("backtracking Training completed")
            execute_Astar()
            print("Astar Training completed")
            execute_GBFS()
            print("GBFS Training completed")
            execute_kmeans()
            print("k-means Training completed")
            result = "ALL TRAINED"
        else:
            result = f"{algorithm} algorithm does not exist in {site}"

    elif site.lower() == 'spgg':

        try:
            fetch_data() #fetch data to update to latest data
            print("Fetched data")
        except:
            print("failed to fetch data")
             
        if algorithm.lower() == 'backtracking':
            result = execute_backtracking_spgg()
        else:
            result = f"{algorithm} algorithm does not exist in {site}"


    else:
        result = "Site not found"

    print(result)
    return result

@app.get("/{site}/train/{algorithm}")
async def run_training_script(site: str, algorithm: str , background_tasks: BackgroundTasks):
    # Add the task to the background
    background_tasks.add_task(execute_training, site, algorithm)

    # Respond immediately to the client
    return {"message": "Training has started"}

@app.get("/{site}/get_result/{algorithm}")
def run_get_result(site: str, algorithm: str ,  current_temp: float = None, target_temp: float = None):
    if current_temp is None and target_temp is None:
        return {"error": "Both current_temp and target_temp are missing"}
    if current_temp is None:
        return {"error": "Missing current_temp"}
    if target_temp is None:
        return {"error": "Missing target_temp"}
    
    if site.lower() == 'w512':
        result = get_w512(current_temp, target_temp,algorithm)
        return result
    elif site.lower() == 'spgg':
        result = get_spgg(current_temp, target_temp,algorithm)
    else:
        return {"message": "Site not found"}
    
    return result

@app.get("/{site}/getBestSettings")
def run_getBestSettings(site: str, temperature: float):
    if site.lower() == 'w512':
        result = execute_w512_getBestSettings(temperature)
        return result
    if site.lower() == 'spgg':
        result = execute_spgg_getBestSettings(temperature)
        return result
    else:
        return {"message": "Site does not exist"}
    
@app.get("/{site}/kmeans")
def run_get_result(site: str,temp: float,humid: float,outdoortemp: float,outdoorhumid: float,co2: float):
    if site.lower() == 'w512':
        result = get_kmeans_result(temp,humid,outdoortemp,outdoorhumid,co2)
        return result
    else:
        return {"message": "Site not found"}