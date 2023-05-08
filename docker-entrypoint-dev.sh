#!/bin/sh

cd /app
celery -A wudpecker_transcribe.celery_config worker --loglevel=DEBUG&
python3 run.py
