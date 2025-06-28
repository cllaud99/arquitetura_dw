FROM apache/airflow:3.0.2

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --prefer-binary -r /requirements.txt

