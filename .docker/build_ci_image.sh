#!/bin/bash

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <source_folder_path>"
    exit 1
fi

image_name=${2:-ci_image}

source_folder=$1
repo_root=$(git rev-parse --show-toplevel)
pushd "$repo_root" # Navigate to the root of the repository

# Build the image
docker buildx build --platform=linux/amd64 -t "$image_name" --build-arg SOURCE_FOLDER="$source_folder" -f .docker/Dockerfile.ci .

popd # Navigate back to the original directory
