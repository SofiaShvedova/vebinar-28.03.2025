#!/bin/bash
CONTAINER_NAME="business_case_rocket_25-scheduler-1"
CONTAINER_PATH="/opt/airflow/data/images"
HOST_PATH="/home/dev/Downloads/data"
mkdir -p "$HOST_PATH"
docker cp "$CONTAINER_NAME:$CONTAINER_PATH/." "$HOST_PATH"
echo "The data has been uploaded"
