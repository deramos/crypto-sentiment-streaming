FROM python:3.8.12-slim-buster

ENV TZ="Africa/Lagos"

WORKDIR /usr/local/crypto
COPY requirements.txt .

RUN pip3 install --upgrade pip \
  && pip3 install --no-cache-dir --default-timeout=100 -r requirements.txt \
  && rm -rf /root/.cache/pip/*

COPY . ./
ENV PYTHONPATH="$PYTHONPATH:/usr/local/crypto"

EXPOSE 80

# start kakfa-producer, kafka-consumer, and flask api using a single script