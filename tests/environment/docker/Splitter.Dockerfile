FROM python:3.11.4-alpine3.18

ARG KEMUX_KAFKA_ADDRESS

ENV SPLITTER_USER=kemux
ENV SPLITTER_USER_HOME=/home/${SPLITTER_USER}

USER root

RUN \
  useradd \
    -m \
    -d ${SPLITTER_USER_HOME} \
    -s /bin/bash \
    ${SPLITTER_USER}\
  && \
  usermod \
    --append \
    --groups ${SPLITTER_USER}\
    ${SPLITTER_USER}\
  && \
  mkdir \
    -p \
    ${SPLITTER_USER_HOME}/lib \
    ${SPLITTER_USER_HOME}/lib/kemux \
    ${SPLITTER_USER_HOME}/streams \
    ${SPLITTER_USER_HOME}/data

COPY \
  pyproject.toml \
  setup.cfg \
  ${SPLITTER_USER_HOME}/lib/

COPY \
  kemux/ \ 
  ${SPLITTER_USER_HOME}/lib/kemux/

COPY \
  tests/lib/splitter/start.py \
  ${SPLITTER_USER_HOME}/splitter.py

COPY \
  tests/lib/splitter/streams/ \
  ${SPLITTER_USER_HOME}/streams/

RUN \
  chown \
    -R \
    ${SPLITTER_USER}:${SPLITTER_USER}\
    ${SPLITTER_USER_HOME}

USER ${SPLITTER_USER}
WORKDIR ${SPLITTER_USER_HOME}

RUN \
  python \
    -m pip \
      install \
        --upgrade \
        pip \
        setuptools \
        wheel \
        ${SPLITTER_USER_HOME}/lib

ENV KEMUX_STREAMS_DIR=${SPLITTER_USER_HOME}/streams
ENV KEMUX_DATA_DIR=${SPLITTER_USER_HOME}/data
ENV KEMUX_KAFKA_ADDRESS=${KEMUX_KAFKA_ADDRESS}

ENV PYTHONPATH=${SPLITTER_USER_HOME}/.local

CMD \
  python3.11 \
    ${SPLITTER_USER_HOME}/splitter.py \
      worker \
      -l info
