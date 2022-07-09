.PHONY: test bins clean
PROJECT_ROOT = github.com/lszymik/dsl-library

export PATH := $(GOPATH)/bin:$(PATH)

# default target
default: test

PROGS = dsl-library

TEST_ARG ?= -race -v -timeout 5m
BUILD := ./build

# Automatically gather all srcs
ALL_SRC := $(shell find . -name "*.go")

# all directories with *_test.go files in them
TEST_DIRS=.

dsl-library:
	go build ./*.go

bins: dsl-library

test: bins
	@rm -f test
	@rm -f test.log
	@echo $(TEST_DIRS)
	@for dir in $(TEST_DIRS); do \
		go test -coverprofile=$@ "$$dir" | tee -a test.log; \
	done;

clean:
	rm -rf bin
	rm -Rf $(BUILD)
