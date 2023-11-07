#!/bin/bash

export AZURE_STORAGE_CONNECTION_STRING=

tar  --exclude='.[^/]*' --exclude='./__*' -cvf /tmp/cassandra-logparse.tgz .
name=$(git rev-parse --short HEAD)
az storage blob upload -f /tmp/cassandra-logparse.tgz --container-name cassandra-logparse --name $name

url=$(az storage blob url  --container-name cassandra-logparse --name $name)

echo "Copy this url to ansible: ${url}"
