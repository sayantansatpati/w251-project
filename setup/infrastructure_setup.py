import sys
import os
import time
import urllib3
import subprocess
import argparse
import pprint

import SoftLayer
from platform_setup_fab import PlatformSetup


def create_sl_cluster(args):
    print("Creating new cluster: {0}".format(args.cluster))
    '''Creates Softlayer Cluster'''
    client = get_softlayer_client()
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
    if args.no_hdfs:
        disks = [25,]

    master_name = '%s-master' % cluster_name
    slave_names = ['%s-slave%d' % (cluster_name, idx+1) for idx in range(nslaves)]


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

    '''
    pub_ip_master = vs_manager.get_instance(master_id)['primaryIpAddress']
    pri_ip_master = vs_manager.get_instance(master_id)['primaryBackendIpAddress']
    print '[master]primaryIpAddress: ', pub_ip_master
    pub_ip_slaves = [vs_manager.get_instance(slave_id)['primaryIpAddress'] for slave_id in slave_ids]
    pri_ip_slaves = [vs_manager.get_instance(slave_id)['primaryBackendIpAddress'] for slave_id in slave_ids]
    print '[slaves]primaryIpAddress: ', pub_ip_slaves
    '''
    # Returns a tuple of master, slave instances
    return vs_manager.get_instance(master_id), [vs_manager.get_instance(slave_id) for slave_id in slave_ids]


def fetch_existing_sl_cluster(args):
    print("Fetching existing cluster: {0}".format(args.cluster))
    client = get_softlayer_client()
    vs_manager = SoftLayer.VSManager(client)

    instances_all = vs_manager.list_instances()
    #print pprint.pprint(instances_all)
    instances = [instance for instance in instances_all if instance['hostname'].startswith(args.cluster + "-")]
    print("Instances of cluster: {0}".format(args.cluster))
    print pprint.pprint(instances)
    master = [instance for instance in instances if 'master' in instance['hostname']][0]
    slaves = [instance for instance in instances if 'slave' in instance['hostname']]
    return master, slaves


def get_softlayer_client():
    username = os.environ['SL_USER']
    api_key = os.environ['SL_KEY']

    if not username or not api_key:
        print("Softlayer Username / Key not set as system variable, Aborting!!!")
        sys.exit(1)

    client = SoftLayer.Client(username=username, api_key=api_key)
    return client

if __name__ == '__main__':
    '''Creates Cluster in Softlayer based on parameters passed'''
    s = time.time()
    parser = argparse.ArgumentParser(description='Cancel SoftLayer cluster')

    parser.add_argument('cluster', action='store', help='Name of cluster')

    parser.add_argument('-n', dest='nnodes', type=int, action='store', default=3, help='Number of nodes')
    parser.add_argument('-c', dest='cpus', type=int, action='store', default=1, help='Number of cpus')
    parser.add_argument('-m', dest='memory', type=int, action='store', default=1024, help='Memory (MB)')
    parser.add_argument('-d', dest='disk', type=int, action='store', default=100, help='Size of second disk for HDFS')
    parser.add_argument('--data-center', dest='data_center', action='store',default='dal05', help='Datacenter')
    parser.add_argument('--identity-file', dest='identity_file', action='store', default=None, help='Identity file for SSH')

    parser.add_argument('-k', dest='ssh_key', action='store', help='SSH key to use')
    parser.add_argument('--no-hdfs', dest='no_hdfs', action='store_true', help='Skip HDFS install')
    parser.add_argument('-e', '--existing', dest='existing', action='store_true', help='Use Existing Cluster')
    args = parser.parse_args()

    print "Arguments: ", args

    (master_instance, slave_instances) = (None, None)

    if not args.existing:
        if not args.ssh_key:
            print("SSH Key [-k] needs to be passed if not using an existing cluster")
            sys.exit(1)
        (master_instance, slave_instances) = create_sl_cluster(args)
    else:
        (master_instance, slave_instances) = fetch_existing_sl_cluster(args)

    paas = PlatformSetup(args, (master_instance, slave_instances))
    paas.setup()

    e = time.time()
    print("TOTAL TIME TAKEN - mins: {0}, secs: {1}".format((e-s)/60, (e-s)))