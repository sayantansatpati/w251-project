import os

import swiftclient

authurl = os.environ['SWIFT_AUTH_URL']
user = os.environ['SWIFT_USER']
key = os.environ['SWIFT_KEY']

conn = swiftclient.client.Connection(
        authurl=authurl, user=user, key=key)
print conn.head_container('w251-enron')
