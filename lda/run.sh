#!/bin/bash
HOSTNAME=`hostname`
/usr/local/spark/bin/spark-submit --class "TopicModel" --master spark://$HOSTNAME:7077 ./target/scala-2.10/topicmodeling_2.10-1.0.jar $HOSTNAME enron
