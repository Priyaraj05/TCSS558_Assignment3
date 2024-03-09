#!/bin/bash

# Compile MembershipServer.java
javac MembershipServer.java

# Check if compilation was successful
if [ $? -eq 0 ]; then
    # Run the Membership Server
    java MembershipServer
else
    echo "Compilation failed. Exiting..."
    exit 1
fi

