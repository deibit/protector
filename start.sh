#!/bin/bash

set -e

#exec python bot/alertbot.py &
exec celery -A start worker --beat -l debug
