FROM apache/airflow:2.2.3

COPY ../../requirements.txt .

RUN pip3 install --upgrade pip \
	&& pip3 install  --no-cache-dir -r requirements.txt