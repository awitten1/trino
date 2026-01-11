#!/bin/bash


docker run -d --name clickhouse-server \
    --ulimit nofile=262144:262144 \
    -v "$PWD/data/clickhouse:/var/lib/clickhouse" \
    -v "$PWD/logs/clickhouse:/var/log/clickhouse-server" \
    -p 8123:8123 \
    -p 9000:9000 \
    -e CLICKHOUSE_USER=username \
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
    -e CLICKHOUSE_PASSWORD=password \
    clickhouse/clickhouse-server:24

