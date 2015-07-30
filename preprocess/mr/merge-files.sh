#!/bin/bash

CLEAN_LOCAL="/home/hadoop/clean"
TMP="/home/hadoop/merge"

files=$(find $CLEAN_LOCAL -maxdepth 1 -type d | sort | cut -d'/' -f5)

for f in $files
do
	echo -e "\n[$(date +"%T")]Processing Folder: $f"
	s=$(date +"%s")
	
	cmd="hadoop fs -getmerge -nl /lda/input/$f $TMP/$f-merged"
	echo $cmd
	eval $cmd

	cmd="hadoop fs -put $TMP/$f-merged /lda/input/$f/merged"
	echo $cmd
        eval $cmd

	e=$(date +"%s")
	echo "[$(date +"%T")] Took $((e - s)) secs"
done
