#!/bin/bash

# Get your connection string from https://ms.portal.azure.com/#@microsoft.onmicrosoft.com/resource/subscriptions/dd31ecae-4522-468e-8b27-5befd051dd53/resourceGroups/cassandra-images-rg/providers/Microsoft.Storage/storageAccounts/cassandraartifacts/keys
# Then make a copy of the file, add the key, and use for deployments

export AZURE_STORAGE_CONNECTION_STRING=

tar  --exclude='.[^/]*' --exclude='./__*' -cvf /tmp/cassandra-logparse.tgz .
name=$(git rev-parse --short HEAD)
az storage blob upload -f /tmp/cassandra-logparse.tgz --container-name cassandra-logparse --name $name

url=$(az storage blob url  --container-name cassandra-logparse --name $name)

echo "Copy this url to ansible: ${url}"
