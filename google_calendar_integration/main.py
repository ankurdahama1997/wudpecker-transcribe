from fastapi import FastAPI, Request
import os 
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

from google_calendar_integration.celery_config import celery_app, start_watch, incoming_ping

load_dotenv()

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Things work"}

@app.get("/watch/{user_uuid}")
async def watch(user_uuid: str, callback: str = Query(None), token: str = Query(None)):
    task = start_watch.delay(token, callback, user_uuid)
    return {"task_id": task.id}


@app.post("/ping")
async def ping(request: Request):
    
    
    channel_id = request.headers.get("x-goog-channel-id", "")
    print(channel_id)
    task = incoming_ping.delay(channel_id)
    print(task.id)
    return {"task_id": task.id}



@app.get("/task/{task_id}")
async def get_task_status(task_id: str):
    task = celery_app.AsyncResult(task_id)
    if task.state == "PENDING":
        return {"status": "PENDING"}
    elif task.state != "FAILURE":
        return {"status": task.state, "result": task.result}
    else:
        return task.state
    