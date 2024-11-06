# use an official Python runtime as a parent image
FROM python:3.11-slim

# install PostgreSQL client library dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/* 

# set the working directory in the container to /app
WORKDIR /app

# copy requirements.txt into the container
COPY requirements.txt .

# install required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# copy the script into the container
COPY PostgreSQL_Database_Schema.py .

# command to run the script
CMD ["python", "PostgreSQL_Database_Schema.py"]

