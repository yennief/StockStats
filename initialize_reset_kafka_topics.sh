 #!/bin/bash

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

kafka-topics.sh --delete --bootstrap-server $CLUSTER_NAME}-w-0:9092 \
 --topic kafka-input
kafka-topics.sh --create --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --replication-factor 2 --partitions 3 --topic kafka-input

kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --topic kafka-output
kafka-topics.sh --create --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --replication-factor 2 --partitions 3 --topic kafka-output

 kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --topic kafka-input-metadata
kafka-topics.sh --create --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --replication-factor 2 --partitions 3 --topic kafka-input-metadata

kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --topic aggregation-input
kafka-topics.sh --create --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --replication-factor 2 --partitions 3 --topic aggregation-input