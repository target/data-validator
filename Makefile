SCALAFMT ?= bin/scalafmt

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
	bin/sbt test

.PHONY: check
check: ## Runs linters and other checks
	bin/sbt scalastyle

.PHONY: build
build:
	bin/sbt assembly

.PHONY: format-scala
format-scala: $(SCALAFMT) ## Formats all Scala code
	$(SCALAFMT)

SCALAFMT_VERSION ?= "$(shell grep "sbt-scalafmt" "build.sbt" | cut -d "%" -f 3 | sed -e 's/[^\.0-9A-Za-z-]//g' )"
$(SCALAFMT):
	coursier bootstrap org.scalameta:scalafmt-cli_2.12:$(SCALAFMT_VERSION) \
		-r sonatype:releases \
		-o $@ \
		--main org.scalafmt.cli.Cli

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

