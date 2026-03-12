#!/bin/bash

# Viper-NYC-Taxi-Modernizer: EMR 7.7.0 Spin-up Script
# Uses local JSON blueprints for repeatable Spark/Iceberg/Infrastructure

echo "Starting EMR 7.7.0 Cluster Provisioning..."

CLUSTER_ID=$(aws emr create-cluster \
--name "Viper-NYC-Taxi-Modernizer" \
--release-label emr-7.7.0 \
--applications Name=Spark Name=Hadoop Name=Livy Name=Hive \
--service-role Teo_EMR_DefaultRole \
--ec2-attributes KeyName=teoNewec2_key,InstanceProfile=Teo_EMR_EC2_InstanceProfile,SubnetId=subnet-00284431d8aebdc28 \
--configurations file://emr_iceberg_config.json \
--instance-groups file://emr_cluster_blueprint.json \
--log-uri s3://teo-nyc-taxi/logs/ \
--bootstrap-actions Name="Install boto3",Path="s3://teo-nyc-taxi/scripts/install-boto3.sh" \
--query 'JobFlowId' --output text)

echo "----------------------------------------------------"
echo "Success! Cluster ID: $CLUSTER_ID"
echo "Status: http://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:$CLUSTER_ID"
echo "----------------------------------------------------"
