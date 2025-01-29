#!/usr/bin/env bash

TOMLS=$(find . -name 'pyproject.toml')
JSON_ARRAY=()
for toml in $TOMLS; do
    if [[ $toml =~ "cache" ]]; then
        continue
    fi
    PACKAGE_PATH=$(dirname $toml)
    PACKAGE_NAME=$(cat $toml | grep -o "name.*=.*" | cut -d'=' -f2 | tr -d '"' | tr -d ' ')
    PACKAGE_VERSION=$(cat $toml | grep -o "version.*=.*" | cut -d'=' -f2 | tr -d '"' | tr -d ' ')
    JSON=$(jq -n --arg name "$PACKAGE_NAME" --arg path "$PACKAGE_PATH" --arg version "$PACKAGE_VERSION" '{name: $name, path: $path, version: $version}')
    JSON_ARRAY+=("$JSON")
done
echo ${JSON_ARRAY[@]} | jq -s