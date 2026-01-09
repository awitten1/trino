#!/bin/bash

set -eux

docker container run -d \
    -p 5432:5432 -e "POSTGRES_USER=${POSTGRES_USER:-postgres}" \
    -e "POSTGRES_DB=${POSTGRES_DB:-postgres}" \
    -e "POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-mypostgrespassword}" \
    postgres:18
