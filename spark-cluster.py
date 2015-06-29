import os
import time
import urllib3
import subprocess
urllib3.disable_warnings()

import SoftLayer

cluster_name = 'test1'
nslaves = 3
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
    return subprocess.check_call(
            ssh_command() + ['-t', '-t', '%s@%s' % (user, host),
                                     stringify_command(command)])

## Create VMs
master_name = '%s-master' % cluster_name
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
master_status = vs_manager.get_instance(master_id)
state = master_status['powerState']['keyName']
while state != 'RUNNING':
    time.sleep(10)
    master_status = vs_manager.get_instance(master_id)
    state = master_status['powerState']['keyName']

"""
vlans = master_status['networkVlans']
for vlan in vlans:
    if vlan['networkSpace'] == 'PRIVATE':
        vlan_id = vlan['id']

slave_opts = []
for idx in range(1, nslaves+1):
    opts = common_opts.copy()
    opts.update({
    'hostname':'%s-slave%d' % (cluster_name, idx),
    'private_vlan':vlan_id,
    })
    slave_opts.append(opts)
create_responses = vs_manager.create_instances(slave_opts)
slave_ids = [response['id'] for response in create_responses]

for response in create_responses:
    slave_id = response['id']
    slave_status = vs_manager.get_instance(slave_id)
    state = slave_status['powerState']['keyName']
    while state != 'RUNNING':
        time.sleep(10)
        slave_status = vs_manager.get_instance(slave_id)
        state = slave_status['powerState']['keyName']

pub_ip_master = vs_manager.get_instance(master_id)['primaryIpAddress']
pri_ip_master = vs_manager.get_instance(master_id)['primaryBackendIpAddress']
pub_ip_slaves = [vs_manager.get_instance(slave_id)['primaryIpAddress'] for slave_id in slave_ids]
pri_ip_slaves = [vs_manager.get_instance(slave_id)['primaryBackendIpAddress'] for slave_id in slave_ids]

"""

ssh(pub_ip_master, 'hostname')


