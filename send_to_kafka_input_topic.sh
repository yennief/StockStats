
#!/bin/bash

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

for file in stocks_result/*.csv; do

output_file=$(bash csvtotxt.sh "$file")
  kafka-console-producer.sh \
 --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --property key.separator=: \
 --property parse.key=true \
 --topic kafka-input  < "$output_file"

done
