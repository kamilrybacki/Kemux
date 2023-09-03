# Deployment as standalone Docker container

Kemux can be deployed as a standalone Docker container.

This is the recommended way of running Kemux in production within a Kubernetes cluster, acting as a switch routing messages from one Kafka topic to another.

## Preparing the Docker image

An example of a Dockerfile can be found within the test directory of the Kemux repository (`tests/environment/docker/Splitter.Dockerfile`).

Python scripts containing Kemux streams can be copied into the image and the path to them can be passed to the `Manager` via a predefined environment variable e.g. `KEMUX_STREAMS_PATH`.

Then, a Python entrypoint can be used to start the `Manager` with the given streams, such as:

```python
import os

from kemux.manager import Manager

if __name__ == "__main__":
    directory_where_streams_are_stored = os.environ["KEMUX_STREAMS_PATH"]
    manager = Manager(
      streams_dir=directory_where_streams_are_stored,
      ...  # Other parameters required by the Manager
    )
    manager.start()
```

An example of such script can also be found within the test directory of the Kemux repository (`tests/lib/splitter.py`).
