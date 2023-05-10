from fastapi import FastAPI, Request, Body
import os 
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
import json

from wudpecker_transcribe.celery_config import celery_app, create_transcript, get_transcript

load_dotenv()

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Things work"}


@app.post("/create")
async def create(request: Request):
    request_body = await request.body()
    request_data = json.loads(request_body)
    call_uuid = request_data.get('call_uuid')
    url = request_data.get('url')
    task = create_transcript.delay(call_uuid, url)
    return {"task_id": task.id}
    
@app.post("/done")
async def done(request: Request):
    validation_token = request.query_params.get('validationToken')
    if validation_token:
        return PlainTextResponse(validation_token)
    request_body = await request.body()
    request_data = json.loads(request_body)
    url = request_data.get('self')
    task = get_transcript.delay(url)
    return {"task_id": task.id}
    
