FROM apache/airflow:2.8.0-python3.10

USER root

# Install Git
RUN apt-get update && apt-get install -y git

USER airflow
ARG GITHUB_TOKEN

ENV GITHUB_TOKEN=$GITHUB_TOKEN


