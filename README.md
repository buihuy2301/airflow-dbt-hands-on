#  AIRFLOW DBT HANDS ON
---
This is my presonal lab using Airflow and DBT to create data pipeline

## Overview
```
.
├── Docker # Contain Dockerfile & requirements to build custom images
├── config # Mount config directory of Airflow containers
├── dags # Mount dags directory of Airflow containers
│   └── utils # contain util code used for dags
├── dbt_demo # dbt project
├── logs # Mount logs directory of Airflow containers
├── plugins # Mount plugins directory of Airflow containers
│   └── operators # Custom operators
└── setup # Contain code logic used for generate mock data and setup lab environment
```

## Prerequisites
Install [Docker desktop]([https://docs.docker.com/engine/install/](https://docs.docker.com/desktop/install/windows-install/))

## Usage
```
docker compose up -d
```
Airflow webserver will expose at localhost:8080
