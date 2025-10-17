FROM python:3.10-bullseye

COPY ./init-spark.sh ./init-spark.sh
COPY ./requirements.txt ./requirements.txt
COPY ./data_preparing.py ./data_preparing.py
COPY ./src ./app/src
COPY ./data ./app/data

RUN sh ./init-spark.sh
ENV PYTHONPATH=$PYTHONPATH:/
WORKDIR ./app