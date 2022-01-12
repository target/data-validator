help: ## Prints help for targets with comments
	@grep -E '^[a-zA-Z._-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

refresh-sbt: ## Retrieve the latest version of sbt launcher
	curl https://raw.githubusercontent.com/paulp/sbt-extras/master/sbt > bin/sbt

UNAME := $(shell uname -s)
ifeq ($(UNAME), Linux)
OS_INFO_CMD=lsb_release -a 2>/dev/null
endif
ifeq ($(UNAME), Darwin)
OS_INFO_CMD=sw_vers
endif

doctor: ## Show important details about compilation environment
	java -version
	$(OS_INFO_CMD)
	bin/sbt version
	git log -1 HEAD --oneline

