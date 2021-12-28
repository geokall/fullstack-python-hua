#!/bin/bash

cd ..
cd opt/
cd kafka
./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list
./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --delete --topic products-topic
./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --delete --topic users-topic
./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --delete --topic __consumer_offsets
./bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list
