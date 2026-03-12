#!/bin/bash
# EMR Bootstrap Action: Install dependencies for the Migration Decorator
set -e

# Install boto3 so the @migration_control_tower can talk to DynamoDB
sudo python3 -m pip install boto3