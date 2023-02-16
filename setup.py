from typing import List

from setuptools import find_packages, setup

__version__ = "0.1.0"

with open("README.md") as f:
    README = f.read()


def get_requirements(filename: str) -> List[str]:
    with open(filename, "r", encoding="utf-8") as fp:
        reqs = [
            x.strip()
            for x in fp.read().splitlines()
            if not x.strip().startswith("#") and not x.strip().startswith("-i")
        ]
    return reqs


setup(
    name="flink-sql-emr-runner-workflows",
    version=__version__,
    description="Framework for scheduling streaming SQL queries on Apache Hadoop YARN and on a standalone Flink cluster.",
    long_description=README,
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    install_requires=get_requirements("deployment-scripts/jobs-deployment/requirements.txt")
)