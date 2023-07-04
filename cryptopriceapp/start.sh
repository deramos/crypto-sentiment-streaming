#/bin/bash

gunicorn --bind 0.0.0.0:9000 --workers 4 --timeout 120 wsgi:app