help: ## Prints help for targets with comments
	@grep -E '^[a-zA-Z._-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

refresh-sbt: ## Retrieve the latest version of sbt launcher
	curl https://raw.githubusercontent.com/paulp/sbt-extras/master/sbt > bin/sbt
