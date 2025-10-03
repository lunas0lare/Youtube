ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10
#pull the apache/airflow image from the website
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
#set an environment variable inside the container to store confids, DAGS, logs...
ENV AIRFLOW_HOME=/opt/airflow
#copy the requirements file to the container's root /
COPY requirements.txt /
#run the requirements file, no cache means: not pop store downloaded packages
RUN  pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt