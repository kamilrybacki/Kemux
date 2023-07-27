FROM python:3.11.4-bullseye

ARG KEMUX_KAFKA_ADDRESS

RUN \
  useradd \
    --create-home \
    --shell /bin/bash \
    consumer

RUN \
  mkdir \
    -p \
      ~/streams \
      ~/kemux

COPY ./tests/environment/consumer/streams/* /home/consumer/streams
COPY ./tests/environment/consumer/start.py /home/consumer/start.py
COPY ./* /home/consumer/kemux

RUN \
  chown \
    -R \
      consumer:producer \
        /home/consumer

USER consumer

RUN \
  pip \
    install \
      ~/kemux

ENV KEMUX_STREAMS_DIR=~/streams
ENV KEMUX_DATA_DIR=/tmp
ENV KEMUX_KAFKA_ADDRESS=${KEMUX_KAFKA_ADDRESS}

CMD [ "python", "~/start.py" ]
