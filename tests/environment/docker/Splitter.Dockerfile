FROM python:3.11.4-alpine3.18

ENV RELEASE_VERSION="0.0.0"
ENV SPLITTER_USER=kemux
ENV SPLITTER_USER_HOME=/home/${SPLITTER_USER}

USER root

RUN \
  mkdir \
    -p \
    ${SPLITTER_USER_HOME}/lib \
    ${SPLITTER_USER_HOME}/lib/kemux \
    ${SPLITTER_USER_HOME}/streams \
    ${SPLITTER_USER_HOME}/data

RUN \
  adduser \
    -h ${SPLITTER_USER_HOME} \
    -s /bin/bash \
    ${SPLITTER_USER} \
    || true

COPY \
  pyproject.toml \
  setup.py \
  README.md \
  ${SPLITTER_USER_HOME}/lib/

COPY \
  kemux/ \ 
  ${SPLITTER_USER_HOME}/lib/kemux/

COPY \
  tests/lib/splitter.py \
  ${SPLITTER_USER_HOME}/splitter.py

COPY \
  tests/streams/ \
  ${SPLITTER_USER_HOME}/streams/

RUN \
  chown \
    -R \
    ${SPLITTER_USER}\
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

ENV STREAMS_DIR=${SPLITTER_USER_HOME}/streams
ENV DATA_DIR=${SPLITTER_USER_HOME}/data

ENV PYTHONPATH=${SPLITTER_USER_HOME}/.local

CMD \
  python3.11 \
    ${SPLITTER_USER_HOME}/splitter.py \
      worker \
      -l info
