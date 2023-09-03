import os
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

version = os.environ.get("RELEASE_VERSION", "0.0.0.dev0")

setuptools.setup(
    name="kemux",
    version=version,
    author="Kamil Rybacki",
    author_email="kamilandrzejrybacki@gmail.com",
    description="Python interface for splitting and/or merging Kafka topics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://kemux.readthedocs.io/en/latest",
    options={
        "packages": setuptools.find_packages(),
        "install_requires": [
            "faust-streaming==0.10.14",
        ],
    }
)
