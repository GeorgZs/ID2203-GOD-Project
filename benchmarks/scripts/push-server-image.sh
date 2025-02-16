#!/usr/bin/env bash
set -eu

println_green() {
    printf "\033[0;32m$1\033[0m\n"
}

source ./project_env.sh # Get PROJECT_NAME and SERVER_DOCKER_IMAGE_NAME env vars
image_name=$SERVER_DOCKER_IMAGE_NAME

println_green "Building server docker image with name '${image_name}'"
docker build --platform linux/amd64 -t "${image_name}" -f  ../../server.dockerfile ../../

println_green "Pushing '${image_name}' to registry"
docker push "${image_name}"

println_green "Done!"
