# syntax=docker/dockerfile:1.22.0
FROM registry.suse.com/bci/bci-base:16.1 AS base

ARG TARGETARCH
ARG http_proxy
ARG https_proxy
ARG SRC_BRANCH=master
ARG SRC_TAG
ARG CACHEBUST

ARG GOLANG_VERSION=1.25.3

ENV ARCH=${TARGETARCH}
ENV GOFLAGS=-mod=vendor

# Install Go
ENV GOPATH=/go PATH=/go/bin:/usr/local/go/bin:${PATH} SHELL=/bin/bash

ENV SRC_BRANCH=${SRC_BRANCH}
ENV SRC_TAG=${SRC_TAG}

RUN for i in {1..10}; do \
        zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:tools:compiler/16.0/devel:tools:compiler.repo && \
        zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:/languages:/python:/Factory/16.0/devel:languages:python:Factory.repo && \
        zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:/languages:/python:/backports/16.0/devel:languages:python:backports.repo && \
        zypper --gpg-auto-import-keys ref && break || sleep 1; \
    done

RUN zypper -n ref && \
    zypper update -y
RUN zypper -n install cmake curl wget gcc13 unzip tar xsltproc docbook-xsl-stylesheets python3 python3-pip fuse3-devel \
              e2fsprogs xfsprogs util-linux-systemd libcmocka-devel device-mapper procps jq git && \
    rm -rf /var/cache/zypp/*

RUN curl -sSL "https://golang.org/dl/go${GOLANG_VERSION}.linux-${ARCH}.tar.gz" -o /tmp/go.tar.gz \
    && tar -C /usr/local -xzf /tmp/go.tar.gz \
    && rm /tmp/go.tar.gz

# Install golangci-lint
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b /usr/local/bin latest

RUN echo "Cloning longhorn/dep-versions SRC_BRANCH=${SRC_BRANCH} SRC_TAG=${SRC_TAG}" && \
    git clone https://github.com/longhorn/dep-versions.git -b ${SRC_BRANCH} /usr/src/dep-versions && \
    cd /usr/src/dep-versions && \
    if [ -n "${SRC_TAG}" ] && git show-ref --tags ${SRC_TAG} > /dev/null 2>&1; then \
        echo "Checking out tag ${SRC_TAG}"; \
        git checkout tags/${SRC_TAG}; \
    fi && \
    echo "dep-versions commit: $(git rev-parse HEAD)"

# Build spdk
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-spdk.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}" "${ARCH}"

# Build libjson-c-devel
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-libjsonc.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}"

# Build nvme-cli
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-nvme-cli.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}"

# Build go-spdk-helper
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-go-spdk-helper.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}"

RUN ldconfig

WORKDIR /go/src/github.com/longhorn/longhorn-spdk-engine
COPY . .

FROM base AS validate
RUN ./scripts/validate && touch /validate.done

FROM scratch AS ci-artifacts
COPY --from=validate /validate.done /validate.done
