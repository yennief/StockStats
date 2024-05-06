 #!/bin/bash

CLUSTER_NAME=$1

D=$2
P=$3
DELAY=$4

java -cp /usr/lib/kafka/libs/*:stock-stats-app.jar StockStatsApp ${CLUSTER_NAME}-w-0:9092 $D $P $DELAY



