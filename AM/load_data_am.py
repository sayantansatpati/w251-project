"""
Run on an EC2 instance with the Enron data mounted as an EBS volume.
"""
import os
import itertools
import swiftclient

# authurl = os.environ['SWIFT_AUTH_URL']
# user = os.environ['SWIFT_USER']
# key = os.environ['SWIFT_KEY']

authurl = "https://dal05.objectstorage.softlayer.net/auth/v1.0/"
user = "SLOS527663-3:arthurmak"
key = "f38a17790a83d57c5a3d1f2f56d5b957ee0fb206cf1db05d47eb77ff66085d7f"

conn = swiftclient.client.Connection(authurl=authurl, user=user, key=key)

zip_file = conn.get_object(container='w251-enron', obj='/enron/edrm-enron-v2/edrm-enron-v2_arnold-j_pst.zip')

print zip_file


# container = conn.get_container('w251-enron')

# for obj in container:
#   print obj

# print container[0]
  # print "{0}\t{1}\t{2}".format(obj.name, obj.size, obj.last_modified)


# print conn.head_container('w251-enron')

# for data in conn.get_container(container_name)[1]:
#         print '{0}\t{1}\t{2}'.format(data['name'], data['bytes'], data['last_modified'])




# root_dir = '/enron'
# folders = [os.path.join(root_dir, folder) for folder in os.listdir(root_dir) if 'enron' in folder]

# folder = folders[0]
# files = [
#           [os.path.join(folder, filename) for filename in os.listdir(folder)]
#           for folder in folders
#         ]
# files = itertools.chain(*files)

# for filename in files:
#     with open(filename, 'r') as fp:
#         print 'Uploading %s' % filename
#         conn.put_object(container='w251-enron', obj=filename, contents=fp, chunk_size=512000)

