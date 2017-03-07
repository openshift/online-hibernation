# Old-skool build tools.

.DEFAULT_GOAL := build

TAG ?= openshift/force-sleep
TARGET ?= prod

DOCKERFILE := Dockerfile
ifeq ($(TARGET),dev)
DOCKERFILE := Dockerfile.local
endif

# Builds and installs the force-sleep binary.
build: check-gopath
	go install \
		github.com/openshift/online/force-sleep/cmd/force-sleep
.PHONY: build


# Runs the unit tests.
#
# Args:
#   TESTFLAGS: Flags to pass to `go test`. The `-v` argument is always passed.
#
# Examples:
#   make test TESTFLAGS="-run TestSomething"
test: build
	go test -v $(TESTFLAGS) \
		github.com/openshift/online/force-sleep/pkg/...
.PHONY: test


# Build a release image. The resulting image can be used with test-release.
#
# Args:
#   TAG: Docker image tag to apply to the built image. If not specified, the
#     default tag "openshift/force-sleep" will be used.
#
# Example:
#   make release TAG="my/force-sleep"
release:
	docker build --rm -f $(DOCKERFILE) -t $(TAG) .
.PHONY: release


# Tests a release image.
#
# Prerequisites:
#   A release image must be built and tagged (make release)
#
# Examples:
#
#   make test-release
#   make test-release TAG="my/force-sleep"
test-release:
	docker run --rm -ti \
		$(DOCKERFLAGS) \
		--entrypoint make \
		$(TAG) \
		test
.PHONY: test-release


# Verifies that source passes standard checks.
verify:
	$(GOPATH)/src/github.com/openshift/online/hack/verify-source.sh
	go vet \
		github.com/openshift/online/force-sleep/cmd/... \
		github.com/openshift/online/force-sleep/pkg/...
.PHONY: verify


# Checks if a GOPATH is set, or emits an error message
check-gopath:
ifndef GOPATH
	$(error GOPATH is not set)
endif
.PHONY: check-gopath
