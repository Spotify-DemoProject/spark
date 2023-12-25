#!/bin/bash

SPARK_FILE="$1"

$SPARK_HOME/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
$SPARK_FILE
