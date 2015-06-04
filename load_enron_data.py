"""
Run on an EC2 instance with the Enron data mounted as an EBS volume.
"""
import os
import itertools

import swiftclient

authurl = os.environ['SWIFT_AUTH_URL']
user = os.environ['SWIFT_USER']
key = os.environ['SWIFT_KEY']

conn = swiftclient.client.Connection(
        authurl=authurl, user=user, key=key)

root_dir = '/enron'
folders = [os.path.join(root_dir, folder) for folder in os.listdir(root_dir) if 'enron' in folder]

folder = folders[0]
files = [
          [os.path.join(folder, filename) for filename in os.listdir(folder)]
          for folder in folders
        ]
files = itertools.chain(*files)

for filename in files:
    with open(filename, 'r') as fp:
        print 'Uploading %s' % filename
        conn.put_object(container='w251-enron', obj=filename, contents=fp, chunk_size=512000)

