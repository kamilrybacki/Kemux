FROM python:3.11.4-bullseye

ARG KEMUX_KAFKA_ADDRESS
ARG KEMUX_BRANCH

USER root

RUN \
  mkdir \
    -p \
      /kemux \
  && \
  git \
    clone \
      --branch \
        ${KEMUX_BRANCH} \
      https://github.com/KamilRybacki/Kemux.git \
      /kemux

RUN \
  pip \
    install \
      /home/splitter/kemux

RUN \
  mkdir \
    -p \
      /opt/kemux-splitter \
  && \
  cp \
    /kemux/tests/lib/splitter/* \
    /opt/kemux-splitter \
  && \
  rm \
    -rf \
      /kemux

ENV KEMUX_STREAMS_DIR=/opt/kemux-splitter/streams
ENV KEMUX_DATA_DIR=/tmp
ENV KEMUX_KAFKA_ADDRESS=${KEMUX_KAFKA_ADDRESS}

CMD [ "python", "/home/splitter/start.py", "worker", "-l", "info" ]
