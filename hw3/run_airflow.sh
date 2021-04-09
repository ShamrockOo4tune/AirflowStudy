#!/bin/bash
cd ~/airflow && source ./.venv/bin/activate
airflow scheduler & sleep 5 && airflow webserver -p 8080 && fg

