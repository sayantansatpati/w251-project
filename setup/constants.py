### Constants

HOSTS_ALL = 'all'
HOSTS_MASTER = 'master'
HOSTS_SLAVES = 'slaves'
POOL_SIZE = 5

# Packages to be installed
PACKAGES = ["screen", "curl", "default-jre", "default-jdk", "nmon", "git", "python-pip", "python-dev"]
PACKAGES_PIP = ["python-swiftclient", "nltk"]

#Spark
SPARK_HOME = "/usr/local/spark"
SPARK_SLAVES_FILE = "/usr/local/spark/conf/slaves"
SPARK_BIN = SPARK_HOME + "/bin"
SPARK_SBIN = SPARK_HOME + "/sbin"
SPARK_MASTER_DAEMON = SPARK_SBIN + "/start-master.sh"
SPARK_SLAVE_DAEMONS = SPARK_SBIN + "/start-slaves.sh"
SPARK_START_ALL_DAEMONS = SPARK_SBIN + "/start-all.sh"
SPARK_STOP_ALL_DAEMONS = SPARK_SBIN + "/stop-all.sh"

#Hadoop
HADOOP_HOME = "/usr/local/hadoop"
HADOOP_CONFIG_HOME = HADOOP_HOME + "/etc/hadoop"

HADOOP_MASTER_FILE = HADOOP_CONFIG_HOME + "/masters"
HADOOP_SLAVES_FILE = HADOOP_CONFIG_HOME + "/slaves"
HADOOP_ENV_FILE = HADOOP_CONFIG_HOME + "/hadoop-env.sh"
HADOOP_YARN_ENV_FILE = HADOOP_CONFIG_HOME + "/yarn-env.sh"
HADOOP_CORE_SITE_FILE = HADOOP_CONFIG_HOME + "/core-site.xml"
HADOOP_MAPRED_SITE_FILE = HADOOP_CONFIG_HOME + "/mapred-site.xml"
HADOOP_HDFS_SITE_FILE = HADOOP_CONFIG_HOME + "/hdfs-site.xml"
HADOOP_YARN_SITE_ENV_FILE = HADOOP_CONFIG_HOME + "/yarn-site.xml"

HADOOP_MAPRED_SITE_TEMPLATE_FILE = HADOOP_CONFIG_HOME + "/mapred-site.xml.template"

# Config
HADOOP_CORE_SITE_CONFIG = ("<property>"
                            "<name>fs.default.name</name>"
                            "<value>hdfs://{0}:9000</value>"
                            "</property>")

HADOOP_MAPRED_SITE_CONFIG = ("<property>"
                            "<name>mapreduce.framework.name</name>"
                            "<value>yarn</value>"
                            "</property>")

HADOOP_HDFS_SITE_CONFIG = ("<property>"
                            "<name>dfs.replication</name>"
                            "<value>3</value>"
                            "</property>"
                            "<property>"
                            "<name>dfs.data.dir</name>"
                            "<value>/data</value>"
                            "</property>")


HADOOP_YARN_SITE_CONFIG = ("<property>"
                            "<name>yarn.nodemanager.aux-services</name>"
                            "<value>mapreduce_shuffle</value>"
                            "</property>"
                            "<property>"
                            "<name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>"
                            "<value>org.apache.hadoop.mapred.ShuffleHandler</value>"
                            "</property>"
                            "<property>"
                            "<name>yarn.resourcemanager.resource-tracker.address</name>"
                            "<value>{0}:8025</value>"
                            "</property>"
                            "<property>"
                            "<name>yarn.resourcemanager.scheduler.address</name>"
                            "<value>{0}:8030</value>"
                            "</property>"
                            "<property>"
                            "<name>yarn.resourcemanager.address</name>"
                            "<value>{0}:8050</value>"
                            "</property>")

HADOOP_SBIN = "/usr/local/hadoop/sbin"
HADOOP_DAEMON = HADOOP_SBIN + "/hadoop-daemon.sh"
HADOOP_YARN_DAEMON = HADOOP_SBIN + "/yarn-daemon.sh"
HADOOP_JOB_HISTORY_DAEMON = HADOOP_SBIN + "/mr-jobhistory-daemon.sh"
HADOOP_STOP_DFS = HADOOP_SBIN + "/stop-dfs.sh"
HADOOP_STOP_YARN = HADOOP_SBIN + "/stop-yarn.sh"