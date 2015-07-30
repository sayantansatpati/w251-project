#!/bin/bash

for x in {a..z}
do
	cmd="hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar -D mapred.reduce.tasks=0 -D mapred.job.reuse.jvm.num.tasks=5 -input /lda/input/${x}*/merged -output /lda/output/$x -mapper 'preprocess-merged.py' -file ./preprocess-merged.py"
	echo $cmd
	eval $cmd
done

