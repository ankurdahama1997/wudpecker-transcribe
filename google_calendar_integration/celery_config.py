from celery import Celery
import time
import uuid
import json
import os 
from dotenv import load_dotenv
import requests

load_dotenv()


celery_app = Celery(
    "google-calendar-integration",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)

celery_app.conf.task_routes = {
    "google-calendar-integration.tasks.*": {"queue": "google-calendar-integration_queue"},
}

@celery_app.task
def start_watch(refresh_token, callback, user_uuid):
    new_uuid = str(uuid.uuid4())
    watch_body = json.dumps({
        "id": new_uuid,
        "token": os.getenv("GOOGLE_TOKEN"),
        "type": "web_hook",
        "address": os.getenv("EVENT_PING_URL"),
        "params": {
            "ttl": "2592000"
        }
    })
    
    try:
        token, token_type = getToken(refresh_token)
    except Exception as e:
        response_request = requests.post(callback, data={"msg": "Failed"})
        return str(e)
    
    watch_url = "https://www.googleapis.com/calendar/v3/calendars/primary/events/watch"
    request_watch = requests.post(watch_url, data=watch_body, headers={'Content-Type': 'x-www-form-url-encoded', 'Authorization': token_type + " " + token})
    response_watch = json.loads(request_watch.text)
    response = {}
    response["uuid"] = user_uuid
    response["google_sync"] = ""
    response["google_channel"] = response_watch.get('id','')
    response["google_expiry"] = response_watch.get('expiration',1)
    
    response_request = requests.post(callback, data=response)
    return response




######################
## HELPER FUNCTIONS ##
######################

def getToken(refresh_token):
    body = json.dumps({"grant_type": "refresh_token", "refresh_token": refresh_token, "client_secret": os.getenv("GOOGLE_SECRET"),
                        "client_id": os.getenv("GOOGLE_CLIENT_ID")})
    request = requests.post('https://oauth2.googleapis.com/token', headers={'Content-Type': 'x-www-form-url-encoded'}, data=body)
    response = json.loads(request.text)
    token = response.get("access_token", "")
    token_type = response.get("token_type", "Bearer")
    return token,token_type