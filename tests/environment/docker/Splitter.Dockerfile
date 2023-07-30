FROM python:3.11.4-bullseye

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
    ${SPLITTER_USER_HOME}/streams \
    ${SPLITTER_USER_HOME}/data

COPY \
  kemux/* \
  pyproject.toml \
  setup.cfg \
  ${SPLITTER_USER_HOME}/lib/

COPY \
  tests/lib/splitter/start.py \
  ${SPLITTER_USER_HOME}/splitter.py

COPY \
  tests/lib/splitter/streams/* \
  ${SPLITTER_USER_HOME}/streams/

RUN \
  chown \
    -R \
    ${SPLITTER_USER}:${SPLITTER_USER}\
    ${SPLITTER_USER_HOME}

USER ${SPLITTER_USER}
WORKDIR ${SPLITTER_USER_HOME}

RUN \
  python3.11 \
    -m pip \
      install \
        --upgrade \
        pip \
        setuptools \
        wheel \
  && \
  python3.11 \
    -m pip \
      install \
      ${SPLITTER_USER_HOME}/lib \
  && \
  rm \
    -rf \
    ${SPLITTER_USER_HOME}/lib

ENV KEMUX_STREAMS_DIR=${SPLITTER_USER_HOME}/streams
ENV KEMUX_DATA_DIR=${SPLITTER_USER_HOME}/data
ENV KEMUX_KAFKA_ADDRESS=${KEMUX_KAFKA_ADDRESS}

CMD \
  python3.11 \
    ${SPLITTER_USER_HOME}/splitter.py \
      worker \
      -l info
