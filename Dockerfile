FROM apache/airflow:2.10.3

# Add required Python packages to image
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --progress-bar off -r /requirements.txt
