FROM python:3.11.4-bullseye

ARG KEMUX_KAFKA_ADDRESS
ENV KEMUX_KAFKA_ADDRESS=${KEMUX_KAFKA_ADDRESS}

ARG KAFKA_BRANCH

USER root

RUN \
  git \
    clone \
      https://github.com/KamilRybacki/Kemux.git \
  && \
  cd \
    /Kemux \
  && \
  git \
    checkout \
      ${KAFKA_BRANCH}

RUN \
  ls \
    -la \
      /Kemux \
  && \
  mkdir \
    -p \
      /opt/kemux-producer \
  && \
  cp \
    /Kemux/tests/lib/producer/start.py \
    /opt/kemux-producer/start.py \
  && \
  rm \
    -rf \
      /Kemux

RUN \
  pip \
    install \
      pymongo \
      kafka-python

CMD [ "python", "/opt/kemux/tests/lib/producer/start.py"]
