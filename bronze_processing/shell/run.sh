#!/bin/bash

fileType=$1
fileLocation=$2
tableName=$3
timeStamp=$(date '+%y%m%d%H%M')

sudo spark-submit \
--jars /home/hadoop/bronze_processing/lib/commonregex-scala_2.10-0.0.1.jar \
--class org.frb.bronze.process.fileCheck /home/hadoop/bronze_processing/target/scala-2.11/bronze_processing_2.11-0.0.1.jar \
${fileType} \
${fileLocation} \
${tableName} \
${USER} \
${timeStamp}
