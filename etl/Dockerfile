# use the Apache Airflow base image
FROM apache/airflow:2.5.1-python3.9

# copy DAGs and requirements
COPY ETL_Pipeline.py /usr/local/airflow/dags/
COPY requirements.txt /requirements.txt

# install dependencies
RUN pip install -r /requirements.txt

# run Airflow
CMD ["airflow", "scheduler"]