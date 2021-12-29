#!/bin/bash

cd ..
cd opt/
cd kafka
./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list
./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --delete --topic products-topic
./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --delete --topic users-topic
./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --delete --topic __consumer_offsets
./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list

#topic __consumer_offsets is automatically created by kafka
#__consumer_offsets is used to store information about committed offsets for each topic:partition per group of consumers (groupID).
# It is compacted topic, so data will be periodically compressed and only latest offsets information available.
