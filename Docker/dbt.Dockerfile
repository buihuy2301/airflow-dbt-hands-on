ARG py_version=3.11.2

FROM python:$py_version-slim-buster as base

ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

FROM base as dbt-core

ARG commit_ref=main

HEALTHCHECK CMD dbt --version || exit 1

WORKDIR /usr/app/

RUN apt-get update && apt-get install -qq -y \
    git gcc build-essential libpq-dev --fix-missing --no-install-recommends \ 
    && apt-get clean

RUN pip install --upgrade pip
RUN mkdir -p /root/.dbt

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# COPY ../../dbt_demo/profiles.yml /root/.dbt/profiles.yml
