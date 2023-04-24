#!/bin/sh

cd /app
celery -A google_calendar_integration.celery_config worker --loglevel=DEBUG&
python3 run.py
