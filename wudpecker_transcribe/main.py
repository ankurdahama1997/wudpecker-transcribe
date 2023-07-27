from fastapi import FastAPI, Request, Body
import os 
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
import json

from wudpecker_transcribe.celery_config import celery_app, create_transcript, get_transcript, deepgram_transcribe, create_transcript_manual

load_dotenv()

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Things work"}


@app.post("/create")
async def create(request: Request):
    request_body = await request.body()
    request_data = json.loads(request_body)
    uuid = request_data.get('uuid')
    url = request_data.get('url')
    lang = request_data.get('lang', 'NaN')
    if lang == 'Nan':
        task = create_transcript.delay(uuid, url)
    else:
        task = create_transcript_manual.delay(uuid, url, lang)
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
    
@app.post("/deepgram/start")
async def deepgram_start(request: Request):
    request_body = await request.body()
    request_data = json.loads(request_body)
    uuid = request_data.get('uuid')
    url = request_data.get('url')
    lang_string = request_data.get('lang','')
    langs = [] if lang_string == '' else list(set(lang_string.split(',')))
    task = deepgram_transcribe.delay(uuid, url, langs)
    return {"task_id": task.id}
    