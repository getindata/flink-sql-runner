ARG FLINK_VERSION
FROM flink:${FLINK_VERSION}-scala_2.12-java8
ARG FLINK_VERSION

# Install Python
# https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/resource-providers/standalone/docker/#using-flink-python-on-docker
RUN apt-get update -y && \
    apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev && \
    wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz && \
    tar -xvf Python-3.7.9.tgz && \
    cd Python-3.7.9 && \
    ./configure --without-tests --enable-shared && \
    make -j6 && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && rm -f Python-3.7.9.tgz && rm -rf Python-3.7.9 && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/flink-sql-runner/
COPY deployment-scripts /opt/flink-sql-runner/deployment-scripts/
COPY python /opt/flink-sql-runner/python/
COPY example /opt/flink-sql-runner/example/

RUN python3 -m pip install \
    -r /opt/flink-sql-runner/deployment-scripts/jobs-deployment/requirements.txt \
    -r /opt/flink-sql-runner/python/requirements.txt \
    apache-flink==${FLINK_VERSION} \
    apache-flink-libraries==${FLINK_VERSION}

RUN python3 -m pip install typing-extensions --upgrade || true

ENTRYPOINT ["sleep", "infinity"]
