import os
import time
import urllib3
import subprocess

import SoftLayer

cluster_name = 'test1'
nslaves = 3

master_name = '%s-master' % cluster_name
slave_names = ['%s-slave%d' % (cluster_name, idx+1) for idx in range(nslaves)]
identity_file = None
user = 'root'

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

def ssh(host, command):
    proc = subprocess.Popen(
            ssh_command() + ['-t', '-t', '%s@%s' % (user, host),
                                     stringify_command(command)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    returncode = proc.poll()
    return returncode, stdout, stderr

## Create VMs
username = os.environ['SL_USER']
api_key = os.environ['SL_KEY']

common_opts = {
    'datacenter':'dal05',
    'domain':'softlayer.com',
    'cpus':1,
    'memory':1024,
    'hourly':True,
    'os_code':'UBUNTU_LATEST',
    'local_disk':True,
    'ssh_keys':[227713,],
    }

master_opts = common_opts.copy()
master_opts.update({
    'hostname':master_name,
    })
master_opts = [master_opts,]

client = SoftLayer.Client(username=username, api_key=api_key)
vs_manager = SoftLayer.VSManager(client)

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

#pub_ip_master = '50.97.65.227'
#pub_ip_slaves = ['50.97.65.228',]
#pri_ip_master = '10.61.47.131'
#pri_ip_slaves = ['10.61.47.148',]

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
command = 'apt-get install -y openjdk-7-jre-headless curl'
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



