from celery import Celery
import json
import os 
from dotenv import load_dotenv
import requests
import boto3
import isodate
import copy

load_dotenv()


celery_app = Celery(
    "wudpecker-transcribe",
    broker=f"redis://{os.getenv('REDIS_URL')}:6379/3",
    backend=f"redis://{os.getenv('REDIS_URL')}:6379/3",
)

celery_app.conf.task_routes = {
    "wudpecker-transcribe.tasks.*": {"queue": "wudpecker-transcribe_queue"},
}

def fail_logger(uuid,msg):
    callback = os.getenv('FAIL_CALLBACK')
    response = requests.post(callback, json={
        "status": "fail",
        "msg": msg,
        "uuid": uuid
    })
    return response

def transcribe_azure_detect_language(url, uuid, langs):
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
                "candidateLocales": langs,
            },
        },
        'locale': langs[0],
        'displayName': uuid})
    azure_key = os.getenv('AZURE_KEY')
    azure_request = requests.post('https://northeurope.api.cognitive.microsoft.com/speechtotext/v3.1/transcriptions', headers={
                                'Content-Type': 'application/json', 'Ocp-Apim-Subscription-Key': azure_key}, data=azure_req_body)
    return azure_request.text




def transcribe_azure_manual(url, uuid, lang):
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
            },
        'locale': lang,
        'displayName': uuid})
    azure_key = os.getenv('AZURE_KEY')
    azure_request = requests.post('https://northeurope.api.cognitive.microsoft.com/speechtotext/v3.1/transcriptions', headers={
                                'Content-Type': 'application/json', 'Ocp-Apim-Subscription-Key': azure_key}, data=azure_req_body)
    return azure_request.text


@celery_app.task
def create_transcript(uuid, url):
    try:
        callback = os.getenv("CREATED_CALLBACK_URL")
        langs = [
                    "en-US",
                    "da-DK",
                    "fr-FR",
                    "de-DE",
                    "pt-BR",
                    "ru-RU",
                    "es-ES",
                    "sv-SE",
                ]

        transcript = transcribe_azure_detect_language(url, uuid, langs)
        response_request = requests.post(callback, data=transcript)
        return transcript
    except Exception as e:
        fail_logger(uuid,f"create_transcript failed: {e}")
        raise


@celery_app.task
def create_transcript_manual(uuid, url, lang):
    try:
        callback = os.getenv("CREATED_CALLBACK_URL")
        transcript = transcribe_azure_manual(url, uuid, lang)
        response_request = requests.post(callback, data=transcript)
        return transcript
    except Exception as e:
        fail_logger(uuid,f"create_transcript_manual failed: {e}")
        raise

def lang_in_langs(lang, langs):
    return (lang in langs or lang.split('-')[0] in langs)

@celery_app.task
def deepgram_transcribe(uuid, url, langs=[]):
    try:
        DEEPGRAM_LANGS = ['da', 'nl', 'en', 'en-US', 'nl', 'fr', 'de', 'hi', 'it', 'ja', 'ko', 'no', 'pl', 'pt', 'pt-BR', 'pt-PT', 'es', 'es-419', 'ta', 'sv']
        NOVA_LANGS = ['en-US','es','en']
        nova = all(lang_in_langs(lang, NOVA_LANGS) for lang in langs)
        callback = os.getenv("DONE_CALLBACK_URL")
        if (len(langs) == 1 and lang_in_langs(langs[0],DEEPGRAM_LANGS)):
            if langs[0] in DEEPGRAM_LANGS:
                lang_code = langs[0]
            else:
                lang_code = langs[0].split('-')[0]

            transcript = transcribe_deepgram(url, lang_code,nova=nova)
            status = 'DEEPGRAM_SINGLE'
        elif len(langs) == 1 and not lang_in_langs(langs[0],DEEPGRAM_LANGS):
            res = transcribe_azure_manual(url, uuid, langs[0])
            if "self" not in res:
                raise ValueError(f"Azure failed: {res}")
            #print(res, flush=True)
            status = 'AZURE_SINGLE'
            data = {"uuid": uuid, "status":status}
            return json.dumps(data)
        elif (len(langs) > 1 and all(lang_in_langs(lang, DEEPGRAM_LANGS) for lang in langs)) or len(langs)==0:
            transcript = transcribe_deepgram(url,nova=nova)
            status = 'DEEPGRAM_MULTI'
        else:
            res = transcribe_azure_detect_language(url, uuid, langs)
            if "self" not in res:
                raise ValueError(f"Azure failed: {res}")
            #print(res,flush=True)
            status = 'AZURE_MULTI'
            data = {"uuid": uuid, "status":status}
            return json.dumps(data)

        try:
            # Extract the actual transcript text and words list
            actual_transcript = transcript['results']['channels'][0]['alternatives'][0]['transcript']
            words_list = transcript['results']['channels'][0]['alternatives'][0]['words']

            # Check if transcript is empty or just whitespace, and if words list is empty
            if not actual_transcript.strip() or not words_list:
                data = {"uuid": uuid, "status": "EMPTY"}
                requests.post(callback, data=data)
                return json.dumps(data)
            else:
                formatted = parse_deepgram(transcript)
        except Exception as e:
            #print(transcript, flush=True)
            failed_callback = os.getenv("FAILED_CALLBACK_URL")
            response_request = requests.post(failed_callback, data={"uuid": uuid, "status": "failed", "url": url})
            raise ValueError(f'Deepgram failed: {transcript}')

        speakers = get_matched_speakers(uuid, formatted)
        formatted = speaker_segments(formatted, speakers)
        #formatted['results']["speakers"] = speakers
        json_file_name = uuid + '_final_.json'

        res = boto3.resource("s3", endpoint_url='https://s3.eu-central-1.amazonaws.com')
        s3object = res.Object(os.getenv("BUCKET_NAME"), json_file_name)
        s3object.put(Body=(bytes(json.dumps(formatted).encode('UTF-8'))))

        # # Check if the meeting is coherent using coherency api
        # try:
        #     coherent_res = requests.get(f"{os.getenv('COHERENCY_URL')}/?azure={uuid}")
        #     if not coherent_res.json():
        #         transcribe_azure_detect_language(url, uuid)
        #         return json.dumps({"uuid": uuid, "status":"Incoherent"})
        # except Exception as e:
        #     print(f"Coherency check failed: {str(e)}")

        data = {"uuid": uuid, "status":status}
        response_request = requests.post(callback, data=data)
        return json.dumps(data)
    except Exception as e:
        fail_logger(uuid,f"deepgram failed: {e}")
        raise

def speaker_segments(transcript, speakers_mapping):
    formatted = copy.deepcopy(transcript)
    results = formatted.get("results", {})
    transcripts = results.get("transcripts", [])
    speaker_labels = results.get("speaker_labels", {})

    if not transcripts or not speaker_labels:
        return formatted

    segments = speaker_labels.get("segments", [])

    for segment in segments:
        speaker_label = segment.get("speaker_label")
        
        # Find the speaker information in the mapping list
        speaker_info = next((s for s in speakers_mapping if s['label'] == speaker_label), None)
        if speaker_info:
            speaker_name = speaker_info.get("name", f"Unknown")
            if speaker_name == "Unknown":
                speaker_name = f"Speaker {int(speaker_label.split('_')[-1])+1}"
        else:
            speaker_name = f"Speaker {int(speaker_label.split('_')[-1])+1}"

        segment["speaker_name"] = speaker_name
        for item in segment.get("items", []):
            item.pop("speaker_name", None)

    return formatted

@celery_app.task
def get_transcript(url):
    try:
        callback = os.getenv("DONE_CALLBACK_URL")
        failed_callback = os.getenv("FAILED_CALLBACK_URL")
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
                try:
                    parsed = make_speaker_matcher(combine_multiple_segments(ParseAzure(azure_transcript)))
                    speakers = get_matched_speakers(req_obj['displayName'], parsed)
                    parsed = speaker_segments(parsed, speakers)
                except Exception as e:
                    data = {"uuid": req_obj['displayName'], "status": "EMPTY"}
                    requests.post(callback, data=data)
                    return json.dumps(data)

                # when there are multiple owners in the same call, update the transcript for each
        
                json_file_name = req_obj['displayName'] + '_final_.json'
                res = boto3.resource("s3", endpoint_url='https://s3.eu-central-1.amazonaws.com')
                s3object = res.Object(os.getenv("BUCKET_NAME"), json_file_name)
                s3object.put(Body=(bytes(json.dumps(parsed).encode('UTF-8'))))
        data = {"uuid": req_obj['displayName'], "status":status}
        if status == "Complete":
            response_request = requests.post(callback, data=data)
        #print(json.dumps(data))
        return json.dumps(data)
    except Exception as e:
        fail_logger(req_obj['displayName'],f"get_transcript failed: {e}")
        raise

# HELPER functions to convert Azure format into Stupid wudpecker format
def get_recall(uuid):
    token = "5832f6593b0b2062bdb90ed84c858756ceab9e13"

    url = f"https://api.recall.ai/api/v1/bot/{uuid}/speaker_timeline/"
    headers = {
        "accept": "application/json",
        "Authorization": "token "+token,
    }
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    return data

def get_matched_speakers(uuid, transcript):
    try:
        azure = copy.deepcopy(transcript)
        recall = get_recall(uuid)

        #clean up
        azure = azure["results"]["speaker_labels"]["segments"]
        r = []
        for segment in azure:
            del segment["items"]
            segment["start_time"] = int(float(segment["start_time"]))
            segment["end_time"] = int(float(segment["end_time"]))
            r.append(segment)
        azure = r
        
        rec =[]
        for segment in recall:
            p = {
                "start_time": int(segment["timestamp"]),
                "speaker_name": segment["name"], 
                "user_id": segment["user_id"]
            }
            rec.append(p)
        recall = rec

        speakers = list(set(d['speaker_label'] for d in azure))
        
        speakers.sort()
        
        test_segments = []
        
        for speaker in speakers:
            count = 0
            test_bunch = []
            all_segs_of_speaker = [x for x in azure if x["speaker_label"] == speaker]
            for seg in all_segs_of_speaker:
                delta = seg["end_time"] - seg["start_time"]
                if delta > 10:
                    test_bunch.append(seg)
                    count += 1
                if count > 10:
                    break
                
            test_segments.append(test_bunch)
        
        
        
        for speaker in test_segments:
            for seg in speaker:
                try:
                    azure_start = seg["start_time"]
                    # FIND THE AZURE START IN RECALL
                    for recall_seg in recall:
                        recall_start = recall_seg["start_time"]
                        if azure_start - 4 <= recall_start <= azure_start + 4:
                            seg["speaker_name"] = recall_seg["speaker_name"]
                    if "speaker_name" not in seg:
                        seg["speaker_name"] = "Unknown"
                except:
                    pass
        
        weights = []
        for speaker in test_segments:
            weight = {}
            for test in speaker:
                if test["speaker_name"] not in weight:
                    weight[test["speaker_name"]] = 1
                else:
                    weight[test["speaker_name"]] += 1
            weights.append(weight)
        
        result = []
        
        for weight in weights:
            if weight:
                winner = max(weight, key=lambda k: weight[k])
                result.append(winner)
            
            
        final_result = []

        for i, res in enumerate(result):
            spk_label = 'spk_' + str(i)
            name = res
            result_obj = {
                "label": spk_label, "name": name, "primary": "no"
            }
            final_result.append(result_obj)


        return final_result
    except:
        return []

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

def transcribe_deepgram(s3url, lang=None, nova=False):
    res = requests.get(os.getenv("DEEPGRAM_TOKEN"))
    token = json.loads(res.text)
    deepgram_key = "Token "+token
    if nova:
        deepgram_key = "Token "+os.getenv("PROD_DEEPGRAM")
    model = "nova" if nova else "general-enhanced"
    if lang:
        url = f"https://api.deepgram.com/v1/listen?language={lang}&diarize=true&punctuate=true&utterances=true&numerals=true&model={model}&keywords=Wudpecker:1"
    else:
        url = f"https://api.deepgram.com/v1/listen?detect_language=true&diarize=true&punctuate=true&utterances=true&numerals=true&model={model}&keywords=Wudpecker:1"
    deepgram_request_data = json.dumps(
        {'url': s3url})
    deepgram_request = requests.post(url, headers={'Content-Type': 'application/json', 'Authorization': deepgram_key}, data=deepgram_request_data)
    
    return json.loads(deepgram_request.text)


def parse_deepgram(data):
    raw = data
    new = {"results": { "transcripts": [{"transcript":raw["results"]["channels"][0]["alternatives"][0]["transcript"]}]}}
    speakers = []
    prev_speaker = -1
    tmp_items = []
    segments = []
    for word in raw["results"]["channels"][0]["alternatives"][0]["words"]:
        speaker = word["speaker"]
        if speaker not in speakers:
            speakers.append(speaker)
        if prev_speaker != speaker:
            if tmp_items:
                tmp = {
                    "start_time": tmp_items[0]['start_time'],
                    "end_time": tmp_items[-1]['end_time'],
                    "speaker_label": "spk_"+str(prev_speaker),
                    "items": tmp_items
                }
                segments.append(tmp)
            prev_speaker = speaker
            tmp_items = []
        
        start = f"{word['start']:.2f}"
        end = f"{word['end']:.2f}"
        w = word["punctuated_word"]
        tmp_items.append({
            "start_time": start,
            "end_time": end,
            "speaker_label": "spk_"+str(speaker),
            "content": w,
        })
    else:
        tmp = {
            "start_time": tmp_items[0]['start_time'],
            "end_time": tmp_items[-1]['end_time'],
            "speaker_label": "spk_"+str(speaker),
            "items": tmp_items
        }
        segments.append(tmp)
    new['results']['speaker_labels'] = {
        "speakers": len(speakers),
        "segments": segments,
    }
    return new
