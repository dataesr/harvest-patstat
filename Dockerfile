FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget gnupg2 ca-certificates lsb-release \
    python3.8 python3-pip python3.8-dev libpython3.8 libpython3.8-dev python3-setuptools  python3-dev\
    git g++ jq \
    && rm -rf /var/lib/apt/lists/*

# MongoDB
RUN mkdir -p /usr/share/keyrings \
    && curl -fsSL https://www.mongodb.org/static/pgp/server-5.0.asc \
    | gpg --dearmor -o /usr/share/keyrings/mongodb.gpg

RUN echo "deb [ arch=amd64 signed-by=/usr/share/keyrings/mongodb.gpg ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" \
    > /etc/apt/sources.list.d/mongodb-org.list

RUN apt-get update && apt-get install -y mongodb-org \
    && rm -rf /var/lib/apt/lists/*

# Node
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash - \
    && apt-get install -y nodejs

WORKDIR /src

ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /src