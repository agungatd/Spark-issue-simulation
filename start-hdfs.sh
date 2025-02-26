#!/bin/bash

# -----------------------------------------
# HDFS NameNode Initialization Script
# -----------------------------------------
# This script initializes and starts the HDFS NameNode service.
# It checks if the NameNode has already been formatted; if not,
# it formats the NameNode before starting it.

# Exit immediately if a command exits with a non-zero status
set -e

# Define the NameNode data directory
NAMENODE_DIR="/opt/hadoop/data/nameNode"

# Check if the NameNode has already been formatted
if [ ! -d "$NAMENODE_DIR/current" ]; then
    echo "====================================================="
    echo "🚀 Formatting NameNode as no existing metadata found."
    echo "====================================================="
    hdfs namenode -format -force -nonInteractive
else
    echo "✅ NameNode already formatted. Skipping format step."
fi

# Start the NameNode service
echo "======================================="
echo "🔧 Starting HDFS NameNode Service..."
echo "======================================="
hdfs namenode

