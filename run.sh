#!/usr/bin/env bash

export JAVA_HOME=/usr/jdk64/jdk1.8.0_66
export SPARK_HOME=/usr/hdp/current/spark-client
export HADOOP_CONF_DIR=/etc/hadoop/conf

$SPARK_HOME/bin/spark-submit --master yarn-client --num-executors 5 --executor-memory 8g --files conf.properties --class com.wobot.etl.EsOld2NewSchema --jars elasticsearch-2.1.1.jar,elasticsearch-hadoop-2.2.0.jar,joda-time-2.8.2.jar,guava-18.0.jar,lucene-core-5.3.1.jar,jsr166e-1.1.0.jar,t-digest-3.0.jar,hppc-0.7.1.jar,jackson-core-2.6.2.jar wobot-etl.jar ./conf.properties
