
#!/bin/bash

set -eux

dir=$(realpath $(dirname $0))
pushd $dir

install_dir="trino-server-core-480-SNAPSHOT"

if [ ! -d ${install_dir} ]; then
    cp "$dir/../core/trino-server-core/target/${install_dir}.tar.gz" .
    tar -xzvf ${install_dir}.tar.gz
    cp -r ./etc ${install_dir}
fi

"${install_dir}/bin/launcher" start
# now connect with trino-cli-479 localhost:8080

popd
