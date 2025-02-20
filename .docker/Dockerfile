# Docker complaines if FROM does not resolve to a valid image. Hence the defaults.
ARG UBUNTU_VERSION=22.04
ARG UV_VERSION=0.5.15
ARG SPARK_VERSION=3.5.4

# Get Spark
FROM spark:${SPARK_VERSION}-scala2.12-java17-python3-r-ubuntu AS spark
USER root
RUN mkdir -p /tmp \
  && chmod 777 /tmp \
  && mv /opt/spark// /tmp/spark \
  && mv /opt/java/openjdk /tmp/java

# Get uv
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

# Base image
FROM mcr.microsoft.com/devcontainers/base:ubuntu-${UBUNTU_VERSION}

# Install System Dependencies
RUN apt-get update \
  && apt-get install -y \
  git \
  curl \
  build-essential \
  openjdk-17-jre-headless \
  ca-certificates-java \
  # gnupg2 allows you to sign git commits with GPG
  gnupg2 \
  gcc \
  python3-dev \
  protobuf-compiler \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get clean


# Copy Spark
ENV SPARK_HOME=/opt/spark
COPY --from=spark /tmp/spark ${SPARK_HOME}
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy uv
COPY --from=uv /uv /bin
COPY --from=uv /uvx /bin
ENV PATH=/bin:$PATH

# Change to non-root user
ENV USER=vscode
ENV HOME=/home/$USER
ENV UV_PROJECT_ENVIRONMENT=$HOME/.venv

# Change the username from jovyan to nayvoj and update the user id to 1001, as that is the user id used in the GitHub Actions runner
RUN usermod -l nayvoj jovyan && \
    usermod -u 1001 nayvoj && \
    usermod -l jovyan nayvoj

RUN usermod -u 1001 jovyan