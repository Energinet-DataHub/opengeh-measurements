formatting_header := \033[1m
formatting_command := \033[1;34m
formatting_desc := \033[0;32m
formatting_none := \033[0m

.PHONY: help

.DEFAULT_GOAL := help

## Show help for each of the targets
help:
	@printf "${formatting_header}Available targets:\n"
	@awk -F '## ' '/^## /{desc=$$2}/^[a-zA-Z0-9][a-zA-Z0-9_-]+:/{gsub(/:.*/, "", $$1); printf "  ${formatting_command}%-20s ${formatting_desc}%s${formatting_none}\n", $$1, desc}' $(MAKEFILE_LIST) | sort
	@printf "\n"

## Build a CI image
image:
	@IMAGE_NAME=measurements_ci_image && \
    printf "${formatting_header}Building CI image: ${formatting_command}$$IMAGE_NAME${formatting_none}\n" && \
    docker buildx build --platform=linux/amd64 -t measurements_ci_image -f .docker/Dockerfile.ci .
