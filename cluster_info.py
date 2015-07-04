import os
import argparse

import SoftLayer

parser = argparse.ArgumentParser(
        description='Cancel SoftLayer cluster')
parser.add_argument('cluster', action='store',
        help='Name of cluster')

args = parser.parse_args()

cluster_name = '%s-' % args.cluster

username = os.environ['SL_USER']
api_key = os.environ['SL_KEY']

client = SoftLayer.Client(username=username, api_key=api_key)
vs_manager = SoftLayer.VSManager(client)

instances = vs_manager.list_instances()
instances = [instance for instance in instances if cluster_name in instance['hostname']]
master = [instance for instance in instances if 'master' in instance['hostname']][0]

for instance in instances:
    print '%s: %s' % (instance['hostname'], instance['primaryIpAddress'])

print 'ssh root@%s' % master['primaryIpAddress']



