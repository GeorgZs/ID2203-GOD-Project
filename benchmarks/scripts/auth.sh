#!/bin/bash

source ./project_env.sh # Get PROJECT_NAME env var

# Set the default project for gcloud commands to the specified project ID.
# This ensures all future gcloud commands will use this project by default,
# so you don't need to specify the --project flag each time.
gcloud config set project $PROJECT_ID

# Log in to your Google account to authenticate the gcloud CLI.
# This opens a browser window for Google account authentication, giving gcloud
# permission to manage resources in your Google Cloud account.
gcloud auth login

# Log in and set up Application Default Credentials (ADC) for gcloud.
# This command generates a set of credentials used by Google libraries and tools that 
# support Application Default Credentials, typically used for local development. 
# ADC allows the application to authenticate seamlessly with Google Cloud services.
gcloud auth application-default login

# Update Docker’s config.json to allow it to use gcloud as a credential helper
# for Google’s container registries (gcr.io, us.gcr.io, eu.gcr.io, and asia.gcr.io).
# This command enables Docker to authenticate with Google Container Registry using
# gcloud-managed credentials, making it easier to push and pull images securely.
gcloud auth configure-docker gcr.io

