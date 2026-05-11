# syntax=docker/dockerfile:1.23.0@sha256:2780b5c3bab67f1f76c781860de469442999ed1a0d7992a5efdf2cffc0e3d769
FROM registry.suse.com/bci/bci-base:16.1@sha256:edf32316d00400ef52c4d0ad80181e0f3d9fd457179017aa0241b7e60ce79998 AS base

ARG TARGETARCH
ARG http_proxy
ARG https_proxy
ARG SRC_BRANCH=master
ARG SRC_TAG
ARG CACHEBUST

ARG GOLANG_VERSION=1.25.3
ENV GOLANGCI_LINT_VERSION=v2.11.4

ENV ARCH=${TARGETARCH}
ENV GOFLAGS=-mod=vendor

# Install Go
ENV GOPATH=/go PATH=/go/bin:/usr/local/go/bin:${PATH} SHELL=/bin/bash

ENV SRC_BRANCH=${SRC_BRANCH}
ENV SRC_TAG=${SRC_TAG}

RUN for i in {1..10}; do \
        zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:tools:compiler/16.0/devel:tools:compiler.repo && \
        zypper --gpg-auto-import-keys ref && break || sleep 1; \
    done

RUN zypper -n ref && \
    zypper update -y

# REMOVE ME: CI workaround for systemd-presets-branding-SLE-15.1-160099.8.1
RUN zypper -n install systemd-presets-branding-SLE_transactional && \
    zypper -n install --no-recommends cmake curl wget gcc13 unzip tar xsltproc docbook-xsl-stylesheets python3 python3-pip fuse3-devel \
              e2fsprogs xfsprogs util-linux-systemd libcmocka-devel device-mapper procps jq git && \
    rm -rf /var/cache/zypp/*

RUN curl -sSL "https://golang.org/dl/go${GOLANG_VERSION}.linux-${ARCH}.tar.gz" -o /tmp/go.tar.gz \
    && tar -C /usr/local -xzf /tmp/go.tar.gz \
    && rm /tmp/go.tar.gz

# Install golangci-lint
RUN curl -fsSL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh -o /tmp/install.sh \
    && chmod +x /tmp/install.sh \
    && /tmp/install.sh -b /usr/local/bin ${GOLANGCI_LINT_VERSION}

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
