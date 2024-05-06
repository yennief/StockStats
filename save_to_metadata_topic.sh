 #!/bin/bash

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

file="$1"
output_file_metadata=$(bash csvtotxtmetadata.sh "$file")

cat "$output_file_metadata" | kafka-console-producer.sh --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --topic kafka-input-metadata --property key.separator=: --property parse.key=true
