#!/bin/bash

set -eux

cat <<EOF | clickhouse-client --user username --password password
create table if not exists y(
    i int,
    j bigint,
    k varchar
) engine = MergeTree()
order by i;

insert into y
select generate_series as i,pow(generate_series,2) as j, pow(generate_series,3)::varchar
from generate_series(0,20000);
EOF

psql "host=localhost port=5432 user=postgres password=mypostgrespassword" -c '
create table if not exists x as select i,(i^2) as j, (i^3) as k from generate_series(0,100000) r(i);
'
