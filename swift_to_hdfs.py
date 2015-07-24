"""
Run on an SoftLayer instance to transfer data from Swift to HDFS
"""
import os
import itertools

import swiftclient
import pydoop.hdfs as hdfs

container = 'w251-enron'
prefix = 'clean_v2'
hdfs_prefix = '/enron'

authurl = os.environ['SWIFT_AUTH_URL']
user = os.environ['SWIFT_USER']
key = os.environ['SWIFT_KEY']

conn = swiftclient.client.Connection(
        authurl=authurl, user=user, key=key)

header, objects = conn.get_container(container, prefix=prefix, full_listing=True)

hdfs.mkdir(hdfs_prefix)

total = len(objects)
count = 1
for obj in objects:
    name = obj['name']
    print 'Downloading %s (%d of %d)' % (name, count, total)
    header, contents = conn.get_object(container, name)
    filename = name.replace('/', '_')
    hdfs.dump(contents, '%s/%s' % (hdfs_prefix, filename))
    count += 1
