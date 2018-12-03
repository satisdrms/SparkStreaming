
path="/usr/hdp/current/kafka-broker"
zookeeper=sandbox-hdp:2181
broker=sandbox-hdp:6667


${path}/bin/kafka-topics.sh --create --zookeeper ${zookeeper}  --topic airports --partitions 1 --replication-factor 1
${path}/bin/kafka-topics.sh --create --zookeeper ${zookeeper}  --topic carriers --partitions 1 --replication-factor 1
${path}/bin/kafka-topics.sh --create --zookeeper ${zookeeper}  --topic planes --partitions 1 --replication-factor 1
${path}/bin/kafka-topics.sh --create --zookeeper ${zookeeper}  --topic flights --partitions 1 --replication-factor 1


