#!/bin/bash

if [ $# == 0 ]; then
    echo "Supply a message"
    exit 1
fi

docker exec broker bash -c "echo \"$1\" | kafka-console-producer --broker-list localhost:9092 --topic Messages --request-required-acks 1"