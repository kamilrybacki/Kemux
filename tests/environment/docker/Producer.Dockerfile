FROM python:3.11.4-bullseye

ARG KEMUX_KAFKA_ADDRESS
ENV KEMUX_KAFKA_ADDRESS=${KEMUX_KAFKA_ADDRESS}

ARG KAFKA_BRANCH

USER root

COPY \
  tests/lib/producer/start.py \
  /producer.py

RUN \
  pip \
    install \
      pymongo \
      kafka-python

CMD [ "python", "/producer.py"]
