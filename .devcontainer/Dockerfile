FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

# Remove the broken Yarn repository from the base image
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
        curl \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Java configuration
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt