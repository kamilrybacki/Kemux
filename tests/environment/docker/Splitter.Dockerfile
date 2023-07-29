FROM python:3.11.4-bullseye

ARG KEMUX_KAFKA_ADDRESS
ARG KEMUX_BRANCH

USER root

RUN \
  useradd \
    -m \
    -s \
      /bin/bash \
    kemux \
  && \
  mkdir \
    -p \
    /home/kemux/lib \
    /home/kemux/streams

COPY \
  kemux/* \
  pyproject.toml \
  setup.cfg \
  /home/kemux/lib/

COPY \
  tests/lib/splitter/start.py \
  /home/kemux/splitter.py

COPY \
  tests/lib/splitter/streams/* \
  /streams/

RUN \
  chown \
    -R \
      kemux:kemux \
    /home/kemux

USER kemux
WORKDIR /home/kemux

RUN \
  pip \
    install \
      ./lib \
  && \
  rm \
    -rf \
    ./lib

RUN \
  unset \
    -v \
      PYTHONPATH

ENV KEMUX_STREAMS_DIR=/streams
ENV KEMUX_DATA_DIR=/tmp
ENV KEMUX_KAFKA_ADDRESS=${KEMUX_KAFKA_ADDRESS}

CMD [ "python", "/home/kemux/splitter.py", "worker", "-l", "info" ]
