FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf /root/.cache/pip/*

COPY ../app /app

EXPOSE 9000

ENV PYTHONPATH="$PYTHONPATH:/app"

CMD ["bash", "./start.sh"]
