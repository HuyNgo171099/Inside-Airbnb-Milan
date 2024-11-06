# use an official lightweight Python image
FROM python:3.11-slim

# install PostgreSQL development packages
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# set the working directory in the container
WORKDIR /app

# create the output directory for the CSV files
RUN mkdir -p /app/output/

# copy the requirements.txt file into the working directory
COPY requirements.txt .

# install the required Python packages
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get remove -y gcc && \
    apt-get autoremove -y

# copy the SQL_Query.py script into the working directory
COPY SQL_Query.py .
COPY wait-for-it.sh .
RUN chmod +x wait-for-it.sh

# set environment variables
ENV DB_PASSWORD=${DB_PASSWORD}

# execute the Python script
CMD ["python", "SQL_Query.py"]

