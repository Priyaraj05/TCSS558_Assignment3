#!/bin/bash
mvn clean && mvn verify
jar_file="target/GenericNode.jar"
destination_directory=$(pwd)
client_directory=$(pwd)/docker_client
server_directory=$(pwd)/docker_server
centralized_directory=$(pwd)/centralized_TCP_membership_server
cp "$jar_file" "$destination_directory"
cp "$jar_file" "$client_directory"
cp "$jar_file" "$server_directory"
cp "$jar_file" "$centralized_directory"
echo "JAR file copied to: $destination_directory"