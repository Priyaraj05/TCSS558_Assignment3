#!/bin/bash
mvn clean && mvn verify
jar_file="target/GenericNode.jar"
destination_directory=$(pwd)
cp "$jar_file" "$destination_directory"
echo "JAR file copied to: $destination_directory"