FROM python:3.11.4-alpine3.18

ARG KEMUX_KAFKA_ADDRESS
ENV KEMUX_KAFKA_ADDRESS=${KEMUX_KAFKA_ADDRESS}

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
