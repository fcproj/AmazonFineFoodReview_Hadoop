#!/bin/bash

#the first is the name of the account, run saldo -b to get it
#PBS -A IscrC_BIGBIOCL
#PBS -l walltime=00:05:00
#PBS -l select=2:ncpus=2:mem=96GB
#PBS -q parallel

## Environment configuration
module load profile/advanced hadoop/2.5.1
export JAVA_HOME=/pico/home/userexternal/fcelli00/java/jdk1.8.0_131
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar

# Configure a new HADOOP instance using PBS job information
$MYHADOOP_HOME/bin/myhadoop-configure.sh -c $HADOOP_CONF_DIR
# Start the Datanode, Namenode, and the Job Scheduler 
$HADOOP_HOME/sbin/start-all.sh 

# compiling the class and creating the JAR
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar

file1=/pico/home/userexternal/fcelli00/hadoop/data/amazon/1999_2006.csv
file2=/pico/home/userexternal/fcelli00/hadoop/data/amazon/2007_2008.csv
file3=/pico/home/userexternal/fcelli00/hadoop/data/amazon/2009_2010.csv
file4=/pico/home/userexternal/fcelli00/hadoop/data/amazon/2011_2012.csv
output_name=result_UserAffinityTwoPasses

# some data movement in hadoop FS 
$HADOOP_HOME/bin/hdfs dfs -mkdir /user
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/fab
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/fab/output
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/fab/input
$HADOOP_HOME/bin/hdfs dfs -put $file1 /user/fab/input
$HADOOP_HOME/bin/hdfs dfs -put $file2 /user/fab/input
$HADOOP_HOME/bin/hdfs dfs -put $file3 /user/fab/input
$HADOOP_HOME/bin/hdfs dfs -put $file4 /user/fab/input

$HADOOP_HOME/bin/hadoop jar $HOME/hadoop/HadoopAmazonReview-1.0.0.jar com/github/fcproj/reviews/affinity/UserAffinityTwoPasses /user/fab/input/ /user/fab/output/$output_name

$HADOOP_HOME/bin/hdfs dfs -get /user/fab/output/$output_name $HOME/hadoop/$output_name
$HADOOP_HOME/bin/hdfs dfs -rm -r /user/fab/input
$HADOOP_HOME/bin/hdfs dfs -rm -r /user/fab/output

# Stop HADOOP services
$MYHADOOP_HOME/bin/myhadoop-shutdown.sh