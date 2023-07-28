FROM python:3.11.4-bullseye

ARG TARGET_KAFKA_ADDRESS
ENV TARGET_KAFKA_ADDRESS=${TARGET_KAFKA_ADDRESS}

COPY ./.local/tests/environment/producer/start.py /opt/start.py

RUN \
  pip \
    install \
      pymongo \
      kafka-python

CMD [ "python", "/opt/start.py"]
