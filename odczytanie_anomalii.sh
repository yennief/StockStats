 #!/bin/bash

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
kafka-console-consumer.sh \
--bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
--topic kafka-output --from-beginning \
--property print.key=true  \
--property key.separator=:

