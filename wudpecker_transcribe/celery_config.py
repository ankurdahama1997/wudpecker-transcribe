from celery import Celery
import time
import uuid
import json
import os 
from dotenv import load_dotenv
import requests
import re
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timezone, timedelta
import boto3
import isodate

load_dotenv()


celery_app = Celery(
    "wudpecker-transcribe",
    broker=f"redis://{os.getenv('REDIS_URL')}:6379/3",
    backend=f"redis://{os.getenv('REDIS_URL')}:6379/3",
)

celery_app.conf.task_routes = {
    "wudpecker-transcribe.tasks.*": {"queue": "wudpecker-transcribe_queue"},
}

@celery_app.task
def create_transcript(call_uuid, url):
    callback = os.getenv("CREATED_CALLBACK_URL")

    azure_req_body = json.dumps(
        {'contentUrls': [url],
        'properties':
            {'diarizationEnabled': True,
            "diarization": {
                "speakers": {
                    "minCount": 1,
                    "maxCount": 6
                }
            },
            'wordLevelTimestampsEnabled': True,
            'punctuationMode': 'DictatedAndAutomatic',
            'profanityFilterMode': 'None',
            "languageIdentification": {
            "candidateLocales": [
                "en-US",
                "fi-FI",
                "da-DK",
                "fr-FR", 
                "de-DE",
                "pt-BR",
                "ru-RU",
                "es-ES",
                "sv-SE",
                "uk-UA"]
                },
            },
        'locale': "en-US",
        'displayName': call_uuid})
    azure_key = os.getenv('AZURE_KEY')
    azure_request = requests.post('https://northeurope.api.cognitive.microsoft.com/speechtotext/v3.1/transcriptions', headers={
                                'Content-Type': 'application/json', 'Ocp-Apim-Subscription-Key': azure_key}, data=azure_req_body)
    
        
    response_request = requests.post(callback, data=azure_request.text)
    print(azure_request)
    return azure_request.text

@celery_app.task
def get_transcript(url):
    callback = os.getenv("DONE_CALLBACK_URL")
    headers = {"Ocp-Apim-Subscription-Key": os.getenv('AZURE_KEY')}

    get_request = requests.get(url, headers=headers)
    req_obj = json.loads(get_request.text)
    files_url = req_obj["links"]["files"]
    files_req = requests.get(files_url, headers=headers)
    files_obj = json.loads(files_req.text)
    status = "Running"
    for file in files_obj["values"]:
        if file.get('kind', 'NaN') == "Transcription":
            status = "Complete"
            json_url = file["links"]["contentUrl"]
            json_download = requests.get(json_url, headers={'Content-Type': 'application/json'})
            azure_transcript = json.loads(json_download.text)

            # parse Transcript
            parsed = make_speaker_matcher(combine_multiple_segments(ParseAzure(azure_transcript)))

            json_file_name = req_obj['displayName'] + '_final_.json'
            res = boto3.resource("s3", endpoint_url='https://s3.eu-central-1.amazonaws.com')
            s3object = res.Object(os.getenv("BUCKET_NAME"), json_file_name)
            s3object.put(Body=(bytes(json.dumps(parsed).encode('UTF-8'))))

    data = {"uuid": req_obj['displayName'], "status":status}
    response_request = requests.post(callback, data=json.dumps(data))
    print(json.dumps(data))
    return json.dumps(data)

# HELPER functions to convert Azure format into Stupid wudpecker format

def PTtoSec(ptime):
    return isodate.parse_duration(ptime).total_seconds()

def MergePunctuations(jdata):
    for index, word in enumerate(jdata["results"]["items"]):
        if index+1 < len(jdata["results"]["items"]):
            if jdata["results"]["items"][index+1]["type"] == "punctuation":
                word["alternatives"][0]["content"] = word["alternatives"][0]["content"] + jdata["results"]["items"][index +
                                                                                                                    1]["alternatives"][0]["content"]

    return jdata

def MakePretty(json_data):

    j_data = MergePunctuations(json_data)

    for segment in j_data["results"]["speaker_labels"]["segments"]:
        for word in segment["items"]:
            start_time = word["start_time"]
            word_here = getWordFromTime(start_time, j_data)
            word["content"] = word_here

    return j_data

def getWordFromTime(time, jdata):
    allWords = filter(
        lambda word: word["type"] == "pronunciation", jdata["results"]["items"])
    found = [word for word in allWords if word["start_time"] == time][0]
    return found["alternatives"][0]["content"]


def combine_multiple_segments(json_obj):
    segments = []
    previous = {}
    c = 0
    for current in json_obj['results']['speaker_labels']['segments']:
        c += 1
        if previous and current['speaker_label'] == previous['speaker_label'] and c != len(json_obj['results']['speaker_labels']['segments']):
            previous['items'].extend(current['items'])
            previous['end_time'] = current['end_time']
        else:
            segments.append(previous)
            previous = current
    segments.append(previous)
    segments.pop(0)
    json_obj['results']['speaker_labels']['segments'] = segments
    return json_obj

def rematch_speakers(aws_speaker, matching):
    for speaker in matching:
        if speaker[0] == aws_speaker:
            return speaker[1]
    return 'spk_100'


def make_speaker_matcher(full_file):
    speakers = []
    match = []
    for segment in full_file["results"]["speaker_labels"]["segments"]:
        if segment['speaker_label'] not in speakers:
            speakers.append(segment['speaker_label'])

    i = 0
    for speaker in speakers:
        match.append((speaker, 'spk_' + str(i)))
        i += 1
    matched_speakers = match
    for segment in full_file["results"]["speaker_labels"]["segments"]:
        segment['speaker_label'] = rematch_speakers(
            segment['speaker_label'], matched_speakers)
        for word in segment['items']:
            word['speaker_label'] = rematch_speakers(
                word['speaker_label'], matched_speakers)
    return full_file


def ParseAzure(data):

    transcript = {}
    transcript["results"] = {}
    transcript["status"] = "AZURE"

    transcript["results"]["transcripts"] = []
    transcript["results"]["transcripts"].append(
        {"transcript": data["combinedRecognizedPhrases"][0]["display"]})

    transcript["results"]["speaker_labels"] = {}
    num_of_speakers = 0
    all_speakers = []

    for phrase in data["recognizedPhrases"]:
        speaker_found = 0
        for speaker in all_speakers:
            if speaker == phrase["speaker"]:
                speaker_found = 1

        if speaker_found == 0:
            all_speakers.append(phrase["speaker"])

    num_of_speakers = len(all_speakers)

    transcript["results"]["speaker_labels"]["speakers"] = num_of_speakers

    transcript["results"]["speaker_labels"]["segments"] = []

    for phrase in data["recognizedPhrases"]:
        phrase_obj = {}
        phrase_obj["start_time"] = str(PTtoSec(phrase["offset"]))
        phrase_obj["end_time"] = str(
            PTtoSec(phrase["offset"]) + PTtoSec(phrase["duration"]))
        phrase_obj["speaker_label"] = "spk_" + str(phrase["speaker"] - 1)
        phrase_obj["items"] = []
        broken_phrase = phrase["nBest"][0]["display"].split()
        broken_phrase_lex = phrase["nBest"][0]["lexical"].split()
        if len(broken_phrase) != len(broken_phrase_lex):
            broken_phrase = broken_phrase_lex
        for idx, word in enumerate(phrase["nBest"][0]["words"]):
            word_obj = {}
            word_obj["start_time"] = str(PTtoSec(word["offset"]))
            word_obj["speaker_label"] = "spk_" + str(phrase["speaker"] - 1)
            word_obj["end_time"] = str(
                PTtoSec(word["offset"]) + PTtoSec(word["duration"]))

            if (idx < len(broken_phrase)):
                word_obj["content"] = broken_phrase[idx]
                phrase_obj["items"].append(word_obj)

        transcript["results"]["speaker_labels"]["segments"].append(phrase_obj)
    return transcript