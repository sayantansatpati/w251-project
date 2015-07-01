import os

import SoftLayer

cluster_name = 'test1'

username = os.environ['SL_USER']
api_key = os.environ['SL_KEY']

client = SoftLayer.Client(username=username, api_key=api_key)
vs_manager = SoftLayer.VSManager(client)

instances = vs_manager.list_instances()
instances = [instance for instance in instances if cluster_name in instance['hostname']]

for instance in instances:
    vs_manager.cancel_instance(instance['id'])
