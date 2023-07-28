FROM python:3.11.4-bullseye

ARG KEMUX_KAFKA_ADDRESS

USER root

RUN \
  useradd \
    --create-home \
    --shell /bin/bash \
    consumer \
  && \
  usermod \
    --append \
    --groups \
      consumer \
    consumer

RUN \
  mkdir \
    -p \
      /home/consumer/streams \
      /home/consumer/kemux

COPY ./.local/tests/environment/consumer/streams/* /home/consumer/streams
COPY ./.local/tests/environment/consumer/start.py /home/consumer/start.py
COPY ./* /home/consumer/kemux

RUN \
  chown \
    -R \
      consumer:consumer \
        /home/consumer

RUN \
  unset \
    -v \
      PYTHONPATH

USER consumer

RUN \
  pip \
    install \
      /home/consumer/kemux

ENV KEMUX_STREAMS_DIR=/home/consumer/streams
ENV KEMUX_DATA_DIR=/tmp
ENV KEMUX_KAFKA_ADDRESS=${KEMUX_KAFKA_ADDRESS}

CMD [ "python", "/home/consumer/start.py", "worker", "-l", "info" ]
