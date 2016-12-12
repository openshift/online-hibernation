.DEFAULT_GOAL := install

.PHONY: install
install:
	go install github.com/openshift/online/force-sleep/cmd/force-sleep

.PHONY: local
local:
	docker build --rm -t openshift/force-sleep:$(tag) -f Dockerfile.local .

.PHONY: test
test:
	go test -v github.com/openshift/online/force-sleep/pkg/...
