FROM python:3.11.4-bullseye

ARG TARGET_KAFKA_ADDRESS
ENV TARGET_KAFKA_ADDRESS=${TARGET_KAFKA_ADDRESS}

COPY ./producer.py /opt/producer.py
COPY ./entrypoint.sh /entrypoint.sh

RUN \
  pip \
    install \
      kafka-python

ENTRYPOINT [ "/entrypoint.sh" ]
