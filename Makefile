SBT ?= bin/sbt

##@ Utilities

.PHONY: help
help: ## Prints help for targets with comments
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development Setup

deps-sys: Brewfile ## Installs system-wide dependencies
	(command -v brew > /dev/null && brew bundle) || true

##@ Development Cycle

.PHONY: test
test: ## Runs tests
	$(SBT) test

.PHONY: check
check: ## Runs linters and other checks
	$(SBT) scalastyle

.PHONY: check-deps
check-deps: ## Checks dependencies are what are expected
	$(SBT) dependencyLockCheck

.PHONY: build
build:
	$(SBT) assembly

.PHONY: relock
relock: ## Lock dependencies based on what's currently referenced
	$(SBT) dependencyLockWrite

.PHONY: format-scala
format-scala: ## Formats all Scala code
	$(SBT) scalafmt

##@ Maintenance Tasks

refresh-sbt: ## Retrieve the latest version of sbt launcher
	curl https://raw.githubusercontent.com/paulp/sbt-extras/master/sbt > bin/sbt

UNAME := $(shell uname -s)
ifeq ($(UNAME), Linux)
OS_INFO_CMD=lsb_release -a 2>/dev/null
endif
ifeq ($(UNAME), Darwin)
OS_INFO_CMD=sw_vers
endif

##@ Debugging

doctor: ## Show important details about compilation environment
	java -version
	$(OS_INFO_CMD)
	bin/sbt version
	git log -1 HEAD --oneline

