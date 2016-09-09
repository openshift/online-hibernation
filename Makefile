.DEFAULT_GOAL := install

.PHONY: install
install:
	go install github.com/openshift/online/force-sleep/cmd/force-sleep

.PHONY: test
test:
	go test -v github.com/openshift/online/force-sleep/pkg/...
