import sys
import os
import time
import urllib3
import subprocess
import argparse

import SoftLayer

parser = argparse.ArgumentParser(
        description='Cancel SoftLayer cluster')
parser.add_argument('cluster', action='store',
        help='Name of cluster')
parser.add_argument('-n', dest='nnodes', type=int, action='store', default=3,
        help='Number of nodes')
parser.add_argument('-c', dest='cpus', type=int, action='store', default=1,
        help='Number of cpus')
parser.add_argument('-m', dest='memory', type=int, action='store', default=1024,
        help='Memory (MB)')
parser.add_argument('-d', dest='disk', type=int, action='store', default=100,
        help='Size of second disk for HDFS')
parser.add_argument('--data-center', dest='data_center', action='store',
        default='dal05', help='Datacenter')
parser.add_argument('-k', dest='ssh_key', action='store',
        required=True, help='SSH key to use')
args = parser.parse_args()

username = os.environ['SL_USER']
api_key = os.environ['SL_KEY']
client = SoftLayer.Client(username=username, api_key=api_key)
vs_manager = SoftLayer.VSManager(client)
ssh_manager = SoftLayer.SshKeyManager(client)

cluster_name = args.cluster
nslaves = args.nnodes - 1
sshkeys = ssh_manager.resolve_ids(args.ssh_key)
memory = args.memory
if args.disk > 0:
    disks = [25, args.disk]
else:
    disks = [25,]
cpus = args.cpus

master_name = '%s-master' % cluster_name
slave_names = ['%s-slave%d' % (cluster_name, idx+1) for idx in range(nslaves)]
identity_file = None

def ssh_args():
    parts = ['-o', 'StrictHostKeyChecking=no']
    parts += ['-o', 'UserKnownHostsFile=/dev/null']
    if identity_file is not None:
        parts += ['-i', identity_file]
    return parts

def ssh_command():
    return ['ssh'] + ssh_args()

def stringify_command(parts):
    if isinstance(parts, str):
        return parts
    else:
        return ' '.join(map(pipes.quote, parts))

def ssh(host, command, user='root'):
    proc = subprocess.Popen(
            ssh_command() + ['-t', '-t', '%s@%s' % (user, host),
                                     stringify_command(command)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    returncode = proc.poll()
    return returncode, stdout, stderr

## Create VMs

common_opts = {
    'datacenter':'dal05',
    'domain':'softlayer.com',
    'cpus':cpus,
    'memory':memory,
    'hourly':True,
    'disks':disks,
    'os_code':'UBUNTU_LATEST',
    'local_disk':True,
    'ssh_keys':sshkeys,
    }

print common_opts
sys.exit(1)

master_opts = common_opts.copy()
master_opts.update({
    'hostname':master_name,
    })
master_opts = [master_opts,]

create_responses = vs_manager.create_instances(master_opts)
response = create_responses[0]

master_id = response['id']
print('Launching master')
vs_manager.wait_for_ready(master_id, limit=600)

master_info = vs_manager.get_instance(master_id)
vlans = master_info['networkVlans']
for vlan in vlans:
    if vlan['networkSpace'] == 'PRIVATE':
        vlan_id = vlan['id']

slave_opts = []
for slave_name in slave_names:
    opts = common_opts.copy()
    opts.update({
    'hostname':slave_name,
    'private_vlan':vlan_id,
    })
    slave_opts.append(opts)

print('Launching slaves')
create_responses = vs_manager.create_instances(slave_opts)
slave_ids = [response['id'] for response in create_responses]
for slave_id in slave_ids:
    vs_manager.wait_for_ready(slave_id, limit=600)

pub_ip_master = vs_manager.get_instance(master_id)['primaryIpAddress']
pri_ip_master = vs_manager.get_instance(master_id)['primaryBackendIpAddress']
pub_ip_slaves = [vs_manager.get_instance(slave_id)['primaryIpAddress'] for slave_id in slave_ids]
pri_ip_slaves = [vs_manager.get_instance(slave_id)['primaryBackendIpAddress'] for slave_id in slave_ids]

print('Setting /etc/hosts')
## Set /etc/hosts content
hostscontent = '127.0.0.1 localhost\n'
hostscontent += '%s %s\n' % (pri_ip_master, master_name)
for slave_name, private_ip in zip(slave_names, pri_ip_slaves):
    hostscontent += '%s %s\n' % (private_ip, slave_name)
command = 'echo "%s" > /etc/hosts' % hostscontent
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

print('Setting up SSH keys')
command = 'ssh-keygen -q -f /root/.ssh/id_rsa -N ""'
returncode, stdout, stderr = ssh(pub_ip_master, command)

command = 'cat /root/.ssh/id_rsa.pub'
returncode, stdout, stderr = ssh(pub_ip_master, command)
pubkey = stdout

command = 'echo "%s" >> /root/.ssh/authorized_keys' % pubkey.decode('ascii')
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

print('Installing Java')
command = 'apt-get install -y curl default-jre default-jdk nmon'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'which java'
returncode, stdout, stderr = ssh(pub_ip_master, command)
command = 'readlink -f %s' % stdout.decode('ascii').strip()
returncode, stdout, stderr = ssh(pub_ip_master, command)
java_home = stdout.decode('ascii').strip().rpartition('/bin/java')[0]
command = 'echo export JAVA_HOME=%s >> /root/.bash_profile' % java_home
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

print('Installing Spark')
spark_url = 'http://d3kbcqa49mib13.cloudfront.net/spark-1.3.1-bin-hadoop2.6.tgz'
outfile = '/root/spark.tgz'
command = 'curl -o %s %s' % (outfile, spark_url)
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = "tar zfx %s -C /usr/local --show-transformed --transform='s,/*[^/]*,spark,'" % outfile
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

spark_home = '/usr/local/spark'
command = 'echo export SPARK_HOME="%s" >> /root/.bash_profile' % spark_home
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

slaves_content = '%s\n' % master_name
for slave_name in slave_names:
    slaves_content += '%s\n' % slave_name
command = 'echo "%s" > /usr/local/spark/conf/slaves' % slaves_content
returncode, stdout, stderr = ssh(pub_ip_master, command)

print('Starting Spark')
command = '%s/sbin/start-master.sh' % spark_home
returncode, stdout, stderr = ssh(pub_ip_master, command)
command = '%s/sbin/start-slaves.sh' % spark_home
returncode, stdout, stderr = ssh(pub_ip_master, command)
print('Spark running at http://%s:8080/' % pub_ip_master)

command = '%s/bin/run-example SparkPi' % spark_home
returncode, stdout, stderr = ssh(pub_ip_master, command)
lines = stdout.decode('ascii').split('\n')
output = [line for line in lines if 'roughly' in line][0]
print(output)

command = 'mkdir -m 777 /data'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

print('Formatting disk')
def find_second_disk(pub_ip):
    command = 'fdisk -l'
    returncode, stdout, stderr = ssh(pub_ip, command)
    lines = stdout.decode('ascii').split('\n')
    lines = [line for line in lines if ('Disk' in line) and ('GB' in line)]
    line = [line for line in lines if 'xvda' not in line][0]
    device = line.split(' ')[1][:-1]
    return device
for pub_ip in [pub_ip_master,]+pub_ip_slaves:
    device = find_second_disk(pub_ip)
    command = 'mkfs.ext4 /dev/xvdc'
    returncode, stdout, stderr = ssh(pub_ip, command)
    fstab_string = "/dev/xvdc /data                   ext4    defaults,noatime        0 0\n"
    command = 'echo "%s" >> /etc/fstab' % fstab_string
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'mount /data'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

print('Installing HDFS')
hadoop_url = 'http://apache.claz.org/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz'
outfile = '/root/hadoop.tgz'
command = 'curl -o %s %s' % (outfile, hadoop_url)
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'tar zfx %s -C /usr/local' % outfile
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'mv /usr/local/hadoop-2.6.0 /usr/local/hadoop'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

"""
command = 'cp /etc/ssh/sshd_config /etc/ssh/sshd_config.orig'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'sed -i "s/#PasswordAuthentication yes/PasswordAuthentication no/" /etc/ssh/sshd_config'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'sed -i "s/ChallengeResponseAuthentication yes/ChallengeResponseAuthentication no/" /etc/ssh/sshd_config'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'service ssh restart'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)
"""

command = 'adduser --disabled-password --gecos "" hadoop'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'echo "hadoop ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'chown -R hadoop.hadoop /data'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'chown -R hadoop.hadoop /usr/local/hadoop'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'cp -a /root/.ssh /home/hadoop/'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)
command = 'chown -R hadoop /home/hadoop/.ssh'
returncode, stdout, stderr = ssh(pub_ip_master, command)
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command)

command = 'echo "export PATH=$PATH:/usr/local/hadoop/bin" >> /home/hadoop/.bash_profile'
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')

command = 'echo "%s" > /usr/local/hadoop/etc/hadoop/masters' % master_name
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')

slavescontent = '%s\n' % master_name
for slave_name in slave_names:
    slavescontent += '%s\n' % slave_name
command = 'echo "%s" > /usr/local/hadoop/etc/hadoop/slaves' % slavescontent
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')

command = 'echo export JAVA_HOME=%s >> /home/hadoop/.bash_profile' % java_home
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')

coresite = '/usr/local/hadoop/etc/hadoop/core-site.xml'
command = 'sed -i "s/<configuration>//" %s' % coresite
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')
command = 'sed -i "s/<\/configuration>//" %s' % coresite
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')

coresite_content = '<configuration>\n<property>\n'
coresite_content += '<name>fs.default.name</name>\n'
coresite_content += '<value>hdfs://%s:54310/</value>' % master_name
coresite_content += '</property>\n</configuration>'
command = 'echo "%s" >> %s' % (coresite_content, coresite)
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')

hdfssite = '/usr/local/hadoop/etc/hadoop/hdfs-site.xml'
command = 'sed -i "s/<configuration>//" %s' % hdfssite
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')
command = 'sed -i "s/<\/configuration>//" %s' % hdfssite
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')
hdfs_content = """
<configuration>
<property>
<name>dfs.replication</name>
<value>3</value>
</property>
<property>
<name>dfs.data.dir</name>
<value>/data</value>
</property>
</configuration>
"""
command = 'echo "%s" >> %s' % (hdfs_content, hdfssite)
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')

set_java = 'export JAVA_HOME=%s' % java_home
command = '%s && /usr/local/hadoop/bin/hadoop namenode -format' % set_java
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')

command = '%s && /usr/local/hadoop/sbin/hadoop-daemon.sh --config /usr/local/hadoop/etc/hadoop --script hdfs start namenode' % set_java
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')

command = '%s && /usr/local/hadoop/sbin/hadoop-daemon.sh --config /usr/local/hadoop/etc/hadoop --script hdfs start datanode' % set_java
returncode, stdout, stderr = ssh(pub_ip_master, command, user='hadoop')
for pub_ip in pub_ip_slaves:
    returncode, stdout, stderr = ssh(pub_ip, command, user='hadoop')

print('HDFS Running at http://%s:50070/dfshealth.jsp' % pub_ip_master)


