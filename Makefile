MACHINE := longhorn
# Define the target platforms that can be used across the ecosystem.
DEFAULT_PLATFORMS := linux/amd64,linux/arm64

export SRC_BRANCH := master
export SRC_TAG := $(shell git tag --points-at HEAD | head -n 1)

export CACHEBUST := $(shell date +%s)

.PHONY: validate test ci

validate:
	docker buildx build --build-arg SRC_BRANCH="$(SRC_BRANCH)" --build-arg SRC_TAG="$(SRC_TAG)" --build-arg CACHEBUST="$(CACHEBUST)" --target validate -f Dockerfile .

test:
	docker buildx build --build-arg SRC_BRANCH="$(SRC_BRANCH)" --build-arg SRC_TAG="$(SRC_TAG)" --build-arg CACHEBUST="$(CACHEBUST)" --target base -t longhorn-spdk-engine-test -f Dockerfile .
	docker run --privileged -v /dev:/host/dev -v /proc:/host/proc -v /sys:/host/sys -v /tmp:/tmp longhorn-spdk-engine-test ./scripts/test

ci:
	docker buildx build --build-arg SRC_BRANCH="$(SRC_BRANCH)" --build-arg SRC_TAG="$(SRC_TAG)" --build-arg CACHEBUST="$(CACHEBUST)" --target base -t longhorn-spdk-engine-test -f Dockerfile .
	docker run --rm --privileged -v /dev:/host/dev -v /proc:/host/proc -v /sys:/host/sys -v /tmp:/tmp -v "$(CURDIR):/output" longhorn-spdk-engine-test sh -c './scripts/ci && cp coverage.out /output/'

.PHONY: buildx-machine
buildx-machine:
	@docker buildx create --name=$(MACHINE) --platform=$(DEFAULT_PLATFORMS) 2>/dev/null || true
	docker buildx inspect $(MACHINE)

.DEFAULT_GOAL := ci
