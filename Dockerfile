FROM apache/airflow:2.0.2
COPY requirements.txt .
RUN pip install -r requirements.txt