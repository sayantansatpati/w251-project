import sys
import os
import urllib3
import time
from StringIO import StringIO
import pprint
from fabric.api import *
from fabric.contrib.files import exists
from fabric.operations import get, put, ssh
from fabric.utils import abort

from constants import *

# Fabric User
env.user = "root"
# Fabric KeyFile
env.key_filename = None
# Add host to known hosts
env.reject_unknown_hosts = False
# No of connection attempts
env.connection_attempts = 3

INSTANCE_DICT = {
    'all': {
        'publicIps': [],
        'privateIps': [],
        'hostnames': []
    },
    'master': {
        'publicIps': [],
        'privateIps': [],
        'hostnames': []
    },
    'slaves': {
        'publicIps': [],
        'privateIps': [],
        'hostnames': []
    }
}


class PlatformSetup(object):

    def __init__(self, args, instances_tuple):
        self.args = args

        self.master_instance = instances_tuple[0]
        self.slave_instances = instances_tuple[1]

        #Set Identity File
        env.key_filename = args.identity_file

        #Populate Instance Dict
        INSTANCE_DICT['all']['publicIps'] = [self.master_instance['primaryIpAddress']]
        INSTANCE_DICT['all']['privateIps'] = [self.master_instance['primaryBackendIpAddress']]
        INSTANCE_DICT['all']['hostnames'] = [self.master_instance['hostname']]

        INSTANCE_DICT['master']['publicIps'] = [self.master_instance['primaryIpAddress']]
        INSTANCE_DICT['master']['privateIps'] = [self.master_instance['primaryBackendIpAddress']]
        INSTANCE_DICT['master']['hostnames'] = [self.master_instance['hostname']]

        for instance in self.slave_instances:
            INSTANCE_DICT['all']['publicIps'].append(instance['primaryIpAddress'])
            INSTANCE_DICT['all']['privateIps'].append(instance['primaryBackendIpAddress'])
            INSTANCE_DICT['all']['hostnames'].append(instance['hostname'])

            INSTANCE_DICT['slaves']['publicIps'].append(instance['primaryIpAddress'])
            INSTANCE_DICT['slaves']['privateIps'].append(instance['primaryBackendIpAddress'])
            INSTANCE_DICT['slaves']['hostnames'].append(instance['hostname'])

        pprint.pprint(INSTANCE_DICT)

    def set_hosts(self, category='all'):
        """Sets env.hosts to a category of hosts"""
        env.hosts = INSTANCE_DICT[category]['publicIps']

    def set_host(self, host):
        """Sets env.hosts to a single host"""
        env.hosts = [host]


    # The following Fabric Tasks are responsible for:
    # Setting up password less ssh across all nodes in the cluster

    @task
    @parallel(pool_size=POOL_SIZE)
    def set_etc_hosts(self):
        hosts_content = '127.0.0.1 localhost\n'
        hosts_content += '%s %s\n' % (INSTANCE_DICT['master']['privateIps'][0],
                                     INSTANCE_DICT['master']['hostnames'][0])
        for i in xrange(self.args.nnodes - 1):
            hosts_content += '%s %s\n' % (INSTANCE_DICT['slaves']['privateIps'][i],
                                         INSTANCE_DICT['slaves']['hostnames'][i])

        run('ls -l /etc/hosts')
        # Delete if exists
        run('rm /etc/hosts')
        cmd = "echo \"{0}\" > /etc/hosts".format(hosts_content)
        run(cmd)
        # Create a new one
        run('cat /etc/hosts')

    @task
    def set_known_hosts(self, hostname):
        for i in xrange(self.args.nnodes):
            print("### Adding {0} to know_hosts".format(INSTANCE_DICT['all']['hostnames'][i]))
            if exists('~/.ssh/known_hosts'):
                # Delete entries if present
                run('ssh-keygen -R {0}'.format(INSTANCE_DICT['all']['hostnames'][i]))
            # Add new entries
            run('ssh-keyscan -H {0} >> ~/.ssh/known_hosts'.format(INSTANCE_DICT['all']['hostnames'][i]))
        run('cat ~/.ssh/known_hosts')

    @task
    def create_keys(self):
        # Create Key(s) if doesn't exist
        if not exists('~/.ssh/id_rsa'):
            run('ssh-keygen -q -t rsa -f /root/.ssh/id_rsa -N ""')
        run("ls -l ~/.ssh/")

    @task
    @parallel(pool_size=POOL_SIZE)
    def set_authorized_keys(self, pub_key_content):
        with cd("~/.ssh"):
            # chmod private key
            run("chmod 700 id_rsa")
            # Add to authorized keys
            run("echo \"{0}\" >> authorized_keys".format(pub_key_content))
            # BAK
            run("mv authorized_keys authorized_keys.BAK")
            #De-Dup
            run("cat authorized_keys.BAK | awk '!x[$0]++' > authorized_keys")


    # The following Fabric Tasks are responsible for:
    # Installing Required Packages

    @task
    @parallel(pool_size=POOL_SIZE)
    def install_packages(self):
        print("Installing Packages, if they are not installed already...")
        with settings(warn_only=True):
            run("apt-get -y update")
            # Uncomment if you want to upgrade all packages
            #run("apt-get -y upgrade")
            for package in PACKAGES:
                result = run("dpkg -s {0}".format(package))
                if result.return_code != 0:
                    print("{0} is not installed, install it now...".format(package))
                    result = run("apt-get -y install {0}".format(package))
                    if result.return_code != 0:
                        abort("ABORTING, PACKAGE INSTALLATION FAILED FOR: {0}".format(package))
                else:
                    print("{0} is already installed...".format(package))

    @task
    def install_sbt(self):
        with settings(warn_only=True):
            result = run("dpkg -s sbt")
            if result.return_code != 0:
                run('echo "deb http://dl.bintray.com/sbt/debian /" >> /etc/apt/sources.list.d/sbt.list')
                run("apt-get -y update")
                run('apt-get install -y --force-yes sbt')
            else:
                print("sbt is already installed...")

    @task
    @parallel(pool_size=POOL_SIZE)
    def install_packages_with_pip(self):
        with settings(warn_only=True):
            for package in PACKAGES_PIP:
                result = run("pip install {0}".format(package))


    @task
    @parallel(pool_size=POOL_SIZE)
    def install_nltk_data(self):
        with settings(warn_only=True):
            if not exists("/usr/share/nltk_data"):
                result = run("python -m nltk.downloader -d /usr/share/nltk_data all")


    @task
    @parallel(pool_size=POOL_SIZE)
    def set_java_home(self):
        java_home = run("which java | xargs readlink -f | awk -F'jre' '{print $1}'")
        print "JAVA_HOME to be set to: %s" %(java_home)
        command = 'echo \"export JAVA_HOME=%s\" >> ~/.bash_profile' % java_home
        print command
        with settings(warn_only=True):
            if exists("~/.bash_profile"):
                result = run("grep JAVA_HOME ~/.bash_profile")
                if result.return_code != 0:
                    print("JAVA_HOME is not set")
                    run(command)
            else:
                run(command)

    # The following Fabric Tasks are responsible for:
    # Installing Spark

    @task
    @parallel(pool_size=POOL_SIZE)
    def install_spark(self):
        print('Installing Spark...')
        if not exists(SPARK_HOME):
            spark_url = 'http://d3kbcqa49mib13.cloudfront.net/spark-1.3.1-bin-hadoop2.6.tgz'
            outfile = '/root/spark.tgz'
            command = 'curl -o %s %s' % (outfile, spark_url)
            result = run(command)

            command = "tar zfx %s -C /usr/local --show-transformed --transform='s,/*[^/]*,spark,'" % outfile
            result = run(command)

            command = "echo \"export SPARK_HOME=%s\" >> /root/.bash_profile" % SPARK_HOME
            result = run(command)
        else:
            print("SPARK is already installed...")

    @task
    def create_spark_slaves_file(self):
        content = ""
        for i in xrange(self.args.nnodes - 1):
            content += '%s\n' % INSTANCE_DICT['slaves']['hostnames'][i]
        print "Content:\n", content
        if not exists(SPARK_SLAVES_FILE):
            run("echo \"{0}\" > {1}".format(content, SPARK_SLAVES_FILE))
        else:
            run("rm {0}".format(SPARK_SLAVES_FILE))
            run("echo \"{0}\" > {1}".format(content, SPARK_SLAVES_FILE))


    @task
    def start_spark(self):
        with settings(warn_only=True):
            result = run(SPARK_STOP_ALL_DAEMONS)

        result = run(SPARK_MASTER_DAEMON)
        result = run(SPARK_SLAVE_DAEMONS)

    @task
    def test_spark(self):
        #Test Spark
        result = run("{0}/run-example SparkPi".format(SPARK_BIN))


    # The following Fabric Tasks are responsible for:
    # Installing Hadoop

    @task
    @parallel(pool_size=POOL_SIZE)
    def create_hadoop_storage(self):
        disk = run("fdisk -l 2>&1 | grep ^Disk | grep GB | grep -v xvda | awk -F[:' '] '{print $2}'")

        if not exists("/data"):
            run('mkdir -m 777 /data')

        with settings(warn_only=True):
            result = run("mount -l | grep '{0}'".format(disk))
            if result.return_code != 0:
                print("Disk hasn't been mounted earlier, mounting it now...")
                run("mkfs.ext4 {0}".format(disk))
                fstab_string = "{0} /data                   ext4    defaults,noatime        0 0\n".format(disk)
                run('echo "{0}" >> /etc/fstab'.format(fstab_string))
                run("mount {0}".format(disk))
            else:
                print("disk is already mounted")


    @task
    @parallel(pool_size=POOL_SIZE)
    def install_hadoop(self):
        print('Installing Hadoop...')
        if not exists(HADOOP_HOME):
            hadoop_url = 'http://apache.claz.org/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz'
            outfile = '/root/hadoop.tgz'
            command = 'curl -o %s %s' % (outfile, hadoop_url)
            result = run(command)

            command = 'tar zfx %s -C /usr/local' % outfile
            result = run(command)

            command = 'mv /usr/local/hadoop-2.6.0 /usr/local/hadoop'
            result = run(command)
        else:
            print("HADOOP is already installed...")


    @task
    @parallel(pool_size=POOL_SIZE)
    def create_hadoop_user_permissions(self):
        with settings(warn_only=True):
            result = run("cut -d: -f1 /etc/passwd | grep hadoop")
            if result.return_code != 0:
                command = 'adduser --disabled-password --gecos "" hadoop'
                result = run(command)

                command = 'echo "hadoop ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers'
                result = run(command)
            else:
                print("User 'hadoop' exists...")

        command = 'chown -R hadoop.hadoop /data'
        result = run(command)

        command = 'chown -R hadoop.hadoop /usr/local/hadoop'
        result = run(command)

        command = 'cp -a /root/.ssh /home/hadoop/'
        if not exists("/home/hadoop/.ssh"):
            result = run(command)
        else:
            # Delete & Create
            result = run("rm -rf /home/hadoop/.ssh")
            result = run(command)

        command = 'chown -R hadoop /home/hadoop/.ssh'
        result = run(command)

    @task
    @parallel(pool_size=POOL_SIZE)
    def update_hadoop_config(self):
        # Create bash_profile
        if exists('/home/hadoop/.bash_profile'):
            result = sudo('rm /home/hadoop/.bash_profile', user="hadoop")
        command = 'echo "export PATH=$PATH:/usr/local/hadoop/bin" >> /home/hadoop/.bash_profile'
        result = sudo(command, user="hadoop")

        java_home = run("which java | xargs readlink -f | awk -F'jre' '{print $1}'")
        print "JAVA_HOME to be set to: %s" %(java_home)
        command = 'echo "export JAVA_HOME=%s" >> /home/hadoop/.bash_profile' % java_home
        result = sudo(command, user="hadoop")

        # Master File
        content = INSTANCE_DICT['master']['hostnames'][0]
        print content
        if not exists(HADOOP_MASTER_FILE):
            result = sudo("echo \"{0}\" > {1}".format(content, HADOOP_MASTER_FILE), user="hadoop")
        else:
            result = sudo("rm {0}".format(HADOOP_MASTER_FILE), user="hadoop")
            result = sudo("echo \"{0}\" > {1}".format(content, HADOOP_MASTER_FILE), user="hadoop")

        # Slaves File
        content = ""
        for i in xrange(self.args.nnodes):
            content += '%s\n' % INSTANCE_DICT['all']['hostnames'][i]
        print "Content:\n", content
        if not exists(HADOOP_SLAVES_FILE):
            result = sudo("echo \"{0}\" > {1}".format(content, HADOOP_SLAVES_FILE), user="hadoop")
        else:
            result = sudo("rm {0}".format(HADOOP_SLAVES_FILE), user="hadoop")
            result = sudo("echo \"{0}\" > {1}".format(content, HADOOP_SLAVES_FILE), user="hadoop")


        # hadoop-env.sh
        result = sudo("sed -ie 's#.*export JAVA_HOME=.*#export JAVA_HOME={0}#g' {1}".format(java_home, HADOOP_ENV_FILE), user="hadoop")

        # yarn-env.sh
        result = sudo("sed -ie 's#.*export JAVA_HOME=.*#export JAVA_HOME={0}#g' {1}".format(java_home, HADOOP_YARN_ENV_FILE), user="hadoop")

        #
        # For following files, create from template for repeatability
        #

        # core-site.xml
        with settings(warn_only=True):
            result = run("rm {0}".format(HADOOP_CORE_SITE_FILE))
        result = run("cp {0} {1}".format(HADOOP_MAPRED_SITE_TEMPLATE_FILE, HADOOP_CORE_SITE_FILE))
        content = HADOOP_CORE_SITE_CONFIG.format(INSTANCE_DICT['master']['hostnames'][0])
        command = "sed -ie '/.*<configuration>.*/{ N; s#\\n#\\n%s\\n# }' %s" %(content, HADOOP_CORE_SITE_FILE)
        result = sudo(command, user="hadoop")

        # mapred-site.xml
        with settings(warn_only=True):
            result = run("rm {0}".format(HADOOP_MAPRED_SITE_FILE))
        result = run("cp {0} {1}".format(HADOOP_MAPRED_SITE_TEMPLATE_FILE, HADOOP_MAPRED_SITE_FILE))
        content = HADOOP_MAPRED_SITE_CONFIG
        command = "sed -ie '/.*<configuration>.*/{ N; s#\\n#\\n%s\\n# }' %s" %(content, HADOOP_MAPRED_SITE_FILE)
        result = sudo(command, user="hadoop")


        # hdfs-site.xml
        with settings(warn_only=True):
            result = run("rm {0}".format(HADOOP_HDFS_SITE_FILE))
        result = run("cp {0} {1}".format(HADOOP_MAPRED_SITE_TEMPLATE_FILE, HADOOP_HDFS_SITE_FILE))
        content = HADOOP_HDFS_SITE_CONFIG
        command = "sed -ie '/.*<configuration>.*/{ N; s#\\n#\\n%s\\n# }' %s" %(content, HADOOP_HDFS_SITE_FILE)
        result = sudo(command, user="hadoop")

        # yarn-site.xml
        with settings(warn_only=True):
            result = run("rm {0}".format(HADOOP_YARN_SITE_ENV_FILE))
        result = run("cp {0} {1}".format(HADOOP_MAPRED_SITE_TEMPLATE_FILE, HADOOP_YARN_SITE_ENV_FILE))
        content = HADOOP_YARN_SITE_CONFIG.format(INSTANCE_DICT['master']['hostnames'][0])
        command = "sed -ie '/.*<configuration>.*/{ N; s#\\n#\\n%s\\n# }' %s" %(content, HADOOP_YARN_SITE_ENV_FILE)
        result = sudo(command, user="hadoop")


    @task
    def format_hadoop_namenode(self):
        # CAUTION: NN is formatted even though there is data (-force used)
        result = sudo("hadoop namenode -format -force -nonInteractive", user="hadoop")


    @task
    def start_hadoop_master(self):
        # Try stopping daemons first
        with settings(warn_only=True):
            sudo("{0} --config {1} --script hdfs stop namenode".format(HADOOP_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

            sudo("{0} --config {1} stop resourcemanager".format(HADOOP_YARN_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

            sudo("{0} --config {1} stop proxyserver".format(HADOOP_YARN_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

            sudo("{0} --config {1} stop historyserver".format(HADOOP_JOB_HISTORY_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

        #Start
        sudo("{0} --config {1} --script hdfs start namenode".format(HADOOP_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

        sudo("{0} --config {1} start resourcemanager".format(HADOOP_YARN_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

        sudo("{0} --config {1} start proxyserver".format(HADOOP_YARN_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

        sudo("{0} --config {1} start historyserver".format(HADOOP_JOB_HISTORY_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

        print("Sleeping for 30 secs for all daemons to come online on master!!!")
        time.sleep(30)
        sudo('jps', user="hadoop")
        print("All Hadoop Daemons started successfully on master!!!")


    @task
    #@parallel(pool_size=POOL_SIZE)
    @serial
    def start_hadoop_slaves(self):
        # Try stopping daemons first
        with settings(warn_only=True):
            sudo("{0} --config {1} stop nodemanager".format(HADOOP_YARN_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

            sudo("{0} --config {1} --script hdfs stop datanode".format(HADOOP_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

            # Avoid incompatible cluster ID problem
            sudo("rm -f /data/current/VERSION", user="hadoop")

        #Start
        sudo("{0} --config {1} start nodemanager".format(HADOOP_YARN_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

        sudo("{0} --config {1} --script hdfs start datanode".format(HADOOP_DAEMON, HADOOP_CONFIG_HOME), user="hadoop")

        sudo('jps', user="hadoop")
        print("All Hadoop Daemons started successfully on this slave!!!")


    @task
    def test_hadoop(self):
        #Test hadoop
        with cd("/usr/local/hadoop/share/hadoop/mapreduce"):
            sudo("hadoop jar hadoop-mapreduce-examples-2.6.0.jar teragen 100000000 /terasort/in")
            sudo("hadoop jar hadoop-mapreduce-examples-2.6.0.jar terasort /terasort/in /terasort/out")
            sudo("hadoop jar hadoop-mapreduce-examples-2.6.0.jar teravalidate /terasort/out /terasort/val")
            sudo("hdfs dfs -rmr /terasort/\*")


    # The following Fabric Tasks are responsible for:
    # Misc

    @task
    @parallel
    def test_task(self):
        run("echo $HOME")
        run('uname -s')


    def setup_password_less_ssh_across_hosts(self):
        # Setup /etc/hosts on all
        res = execute(self.set_etc_hosts, self)

        # Add to known_hosts for each node (Ex: master adds slave1 & slave2)
        for i in xrange(self.args.nnodes):
            self.set_host(INSTANCE_DICT['all']['publicIps'][i])
            res = execute(self.set_known_hosts, self, INSTANCE_DICT['all']['hostnames'][i])

        # Create Key in master
        self.set_hosts(HOSTS_MASTER)
        res = execute(self.create_keys, self)

        # Get Private/Public Key from master
        fd = StringIO()
        env.host_string = INSTANCE_DICT['master']['publicIps'][0]
        get(remote_path='~/.ssh/id_rsa', local_path=".")
        get(remote_path='~/.ssh/id_rsa.pub', local_path=fd)
        print(fd.getvalue())

         # Copy the Private Key downloaded from master (to local) to Slaves
        for i in xrange(self.args.nnodes - 1):
            cmd = "scp -i {0} -o StrictHostKeyChecking=no ./id_rsa root@{1}:~/.ssh/".format(self.args.identity_file,
                                                                INSTANCE_DICT['slaves']['publicIps'][i])
            local(cmd)

        # Set authorized keys in all
        self.set_hosts(HOSTS_ALL)
        res = execute(self.set_authorized_keys, self, fd.getvalue())


    def setup(self):
        """Driver Function for setting up the Platform"""

        self.set_hosts(HOSTS_ALL)
        # Test task
        res = execute(self.test_task, self)

        # Setup password less ssh across all hosts
        self.setup_password_less_ssh_across_hosts()

        # Install Java
        self.set_hosts(HOSTS_ALL)
        res = execute(self.install_packages, self)
        res = execute(self.set_java_home, self)
        res = execute(self.install_packages_with_pip, self)
        res = execute(self.install_nltk_data, self)


        # ------------------------------------------------
        # SPARK (Install & Run as root)
        # ------------------------------------------------

        #Install Spark
        self.set_hosts(HOSTS_ALL)
        res = execute(self.install_spark, self)

        # Create Spark Slaves file in master
        self.set_hosts(HOSTS_MASTER)
        res = execute(self.install_sbt, self)
        res = execute(self.create_spark_slaves_file, self)

        # Start/Stop Spark Cluster. Run Test.
        self.set_hosts(HOSTS_MASTER)
        res = execute(self.start_spark, self)
        res = execute(self.test_spark, self)

        # ------------------------------------------------
        # HADOOP (Run as hadoop after installation as root)
        # ------------------------------------------------

        #Install Hadoop
        self.set_hosts(HOSTS_ALL)
        res = execute(self.install_hadoop, self)
        res = execute(self.create_hadoop_storage, self)
        res = execute(self.create_hadoop_user_permissions, self)

        self.set_hosts(HOSTS_ALL)
        # Switch user to hadoop
        env.user = "hadoop"
        res = execute(self.update_hadoop_config, self)
        self.set_hosts(HOSTS_MASTER)
        res = execute(self.format_hadoop_namenode, self)

        # Start/Stop Hadoop Cluster. Run Test.
        self.set_hosts(HOSTS_MASTER)
        res = execute(self.start_hadoop_master, self)
        self.set_hosts(HOSTS_SLAVES)
        res = execute(self.start_hadoop_slaves, self)
        # Test might take a while (Terasort)
        #res = execute(self.test_hadoop, self)

