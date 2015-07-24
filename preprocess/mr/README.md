# Pre-Processing Enron Data

**Prerequisites**

1. NLTK has to be installed on all nodes of Hadoop/HDFS/Spark Cluster
2. NLTK Data has to be installed under $HOME/nltk_data (Default Dir for installation) 
on all nodes of Hadoop/HDFS/Spark Cluster

**TODO**

Explore the option of installing NLTK from within MR program: http://blog.cloudera.com/blog/2008/11/sending-files-to-remote-task-nodes-with-hadoop-mapreduce/

**Steps Performed**

Hadoop Map-Reduce Streaming has been used to perform the following:

1. Tokenization
2. Stop Words Removal
2. Stemming


### Test Changes in Local (Without using Map-Reduce)
python preprocess.py < ~/datasets/enron/clean_v2_allen-p_1_3.818877.G3T4II30F0UK4YM4G2XQMIIKYS451SXUA.txt


### Test in Cluster (Hadoop Streaming Command Example):

hadoop jar /usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.7.0.jar
-D mapred.reduce.tasks=0
-input /user/cloudera/enron/input
-output /user/cloudera/enron/output
-mapper 'preprocess.py'
-file ./preprocess.py

### Input Files:

[cloudera@localhost mr]$    hadoop fs -ls enron/input
Found 19 items
-rw-r--r--   3 cloudera cloudera       9105 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818877.G3T4II30F0UK4YM4G2XQMIIKYS451SXUA.txt

-rw-r--r--   3 cloudera cloudera       5450 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818878.NHCBYBFSIXWBWWIQBGRIUJM4LHLDT52IA.txt

-rw-r--r--   3 cloudera cloudera        592 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818879.DZTMUR54QW0YWPKBJ4NBJ2XZAZ2DDXXWA.txt

-rw-r--r--   3 cloudera cloudera       1101 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818880.OACQBPZXGWSDHLIQBT4KDHQMS4DBCPUCB.txt

-rw-r--r--   3 cloudera cloudera        604 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818881.BIXTF24YQ5HAEHKWJNEK2EJI0HSIDPOLA.txt

-rw-r--r--   3 cloudera cloudera       1086 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818882.MGU2FJMCXZSEGGJEABG01PYVRWUJZK2EB.txt

-rw-r--r--   3 cloudera cloudera       2314 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818883.N3MRHGA41VFKN4PXLSFFOAD05S5I2O3MA.txt

-rw-r--r--   3 cloudera cloudera        620 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818884.F2O5F320KDAPBFH2VU5351QWQWOTS0M5B.txt

-rw-r--r--   3 cloudera cloudera        596 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818885.JLRYP3ARLIIYUC4AX14ZJE3A1RAPULBFA.txt

-rw-r--r--   3 cloudera cloudera       1471 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818886.LDZQSGT5FQLDRLO1WBHN2MK2DFSDVTACA.txt

-rw-r--r--   3 cloudera cloudera       1049 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818887.JDGOYCD4ZOF4ACLI3CCA2YUX31RN101HA.txt

-rw-r--r--   3 cloudera cloudera        568 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818888.GCVE0AGANCRF55Y2W2CMLZW4KUXUINIZA.txt

-rw-r--r--   3 cloudera cloudera        713 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818889.NOL2FAS3RJJSB2MX2ZYSV2NBC2VJB0WOA.txt

-rw-r--r--   3 cloudera cloudera        746 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818890.JUBNUUSZXHHDJPKVXVFJNGRS4Z2UTWC3A.txt

-rw-r--r--   3 cloudera cloudera       2710 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818891.CCT0WUWQQL3IEAVFD0JX02W0OKBF0EU4B.txt

-rw-r--r--   3 cloudera cloudera       1126 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818892.I5ZSGY5LXSNGO2JIGWYMOP2GO1IX44UAB.txt

-rw-r--r--   3 cloudera cloudera       5246 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818893.DFXQNJMFWSYF2XP2MJGVK503UCDFH5YSB.txt

-rw-r--r--   3 cloudera cloudera        811 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818894.G0PM2HB5IBSYKFT1XHJ1FHAMRIBTEBWMB.txt

-rw-r--r--   3 cloudera cloudera        531 2015-07-12 08:28 enron/input/clean_v2_allen-p_1_3.818895.H4IABOCVW5D3UCO3RM410L5PLPUDXXNMB.txt

### Output Files:
[cloudera@localhost mr]$    hadoop fs -ls enron/output
Found 21 items
-rw-r--r--   3 cloudera cloudera          0 2015-07-12 09:45 enron/output/_SUCCESS

drwxr-xr-x   - cloudera cloudera          0 2015-07-12 09:43 enron/output/_logs

-rw-r--r--   3 cloudera cloudera       5860 2015-07-12 09:43 enron/output/part-00000

-rw-r--r--   3 cloudera cloudera       4340 2015-07-12 09:43 enron/output/part-00001

-rw-r--r--   3 cloudera cloudera       3351 2015-07-12 09:43 enron/output/part-00002

-rw-r--r--   3 cloudera cloudera       1698 2015-07-12 09:43 enron/output/part-00003

-rw-r--r--   3 cloudera cloudera       2254 2015-07-12 09:44 enron/output/part-00004

-rw-r--r--   3 cloudera cloudera       1206 2015-07-12 09:44 enron/output/part-00005

-rw-r--r--   3 cloudera cloudera        775 2015-07-12 09:44 enron/output/part-00006

-rw-r--r--   3 cloudera cloudera        880 2015-07-12 09:44 enron/output/part-00007

-rw-r--r--   3 cloudera cloudera        816 2015-07-12 09:44 enron/output/part-00008

-rw-r--r--   3 cloudera cloudera        835 2015-07-12 09:44 enron/output/part-00009

-rw-r--r--   3 cloudera cloudera        618 2015-07-12 09:44 enron/output/part-00010

-rw-r--r--   3 cloudera cloudera        577 2015-07-12 09:44 enron/output/part-00011

-rw-r--r--   3 cloudera cloudera        535 2015-07-12 09:44 enron/output/part-00012

-rw-r--r--   3 cloudera cloudera        464 2015-07-12 09:44 enron/output/part-00013

-rw-r--r--   3 cloudera cloudera        483 2015-07-12 09:44 enron/output/part-00014

-rw-r--r--   3 cloudera cloudera        459 2015-07-12 09:44 enron/output/part-00015

-rw-r--r--   3 cloudera cloudera        464 2015-07-12 09:44 enron/output/part-00016

-rw-r--r--   3 cloudera cloudera        439 2015-07-12 09:44 enron/output/part-00017

-rw-r--r--   3 cloudera cloudera        415 2015-07-12 09:45 enron/output/part-00018









