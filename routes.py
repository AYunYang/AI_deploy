from fastapi import FastAPI, HTTPException
from fastapi import FastAPI, BackgroundTasks
from training.w512 import execute_w512_training
from get_result.get_w512 import get_w512
from getBestSettiings.w512_getBestSettings import execute_w512_getBestSettings

app = FastAPI()


def execute_training(site: str):
    if site.lower() == 'w512':
        result = execute_w512_training()
        print("completed")
    else:
        result = "Site not found"
    return result

@app.post("/{site}/train")
def run_training_script(site: str, background_tasks: BackgroundTasks):
    # Run the training in the background
    background_tasks.add_task(execute_training, site)
    
    # Respond immediately to the client
    return {"message": "Training started"}


@app.get("/{site}/get_result")
def run_get_result( site: str, current_temp: float = None, target_temp: float = None):

    if site.lower() == 'w512':
        if current_temp is None and target_temp is None:
            return {"error": "Both current_temp and target_temp are missing"}
        if current_temp is None:
            return {"error": "missing current_temp"}
        if target_temp is None:
            return {"error": "missing target_temp"}
        
        result = get_w512(current_temp, target_temp)
        return result
    else:
        result = "Site not found"

    return {"message": result}


    
@app.get("/{site}/getBestSettings")
def run_getBestSettings(site: str, temperature: float):
    if site.lower() == 'w512':
        result = execute_w512_getBestSettings(temperature)
        return result
    else:
        return "Site does not exist"
    