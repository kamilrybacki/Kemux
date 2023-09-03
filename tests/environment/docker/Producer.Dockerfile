FROM python:3.11.4-alpine3.18

USER root

COPY \
  tests/lib/producer.py \
  /producer.py

RUN \
  pip \
    install \
      pymongo \
      kafka-python

CMD [ "python", "/producer.py"]
