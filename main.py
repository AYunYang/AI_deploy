from routes import app
import uvicorn

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("routes:app", host="0.0.0.0", port=8000, reload=True, workers=1) #, limit_concurrency=1, limit_max_requests=1)