FROM apache/airflow:2.7.0

USER root

# Install Git
RUN apt-get update && apt-get install -y git

USER airflow
ARG GITHUB_TOKEN

ENV GITHUB_TOKEN=$GITHUB_TOKEN

COPY requirements.txt .

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install -r requirements.txt


