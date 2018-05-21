#!/bin/bash

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/vlad/output_*
$HADOOP_HOME/bin/hadoop jar /home/vlad/development/KMeans/out/artifacts/KMeans_jar/KMeans.jar kmeans.KMeans &> out_kmeans
