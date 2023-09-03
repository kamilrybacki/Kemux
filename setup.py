import os
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

if version := os.environ.get("RELEASE_VERSION"):
    setuptools.setup(
        name="kemux",
        version=version,
        author="Kamil Rybacki",
        author_email="kamilandrzejrybacki@gmail.com",
        description="Python interface for splitting and/or merging Kafka topics",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://kemux.readthedocs.io/en/latest",
        packages=setuptools.find_packages(),
        install_requires=[
            "faust-streaming==0.10.14",
        ]
    )
else:
    raise RuntimeError("RELEASE_VERSION environment variable is not set")
