#general setup, update, python install, swiftclient install
sudo apt-get update
sudo apt-get install idle
sudo apt-get install python-pip python-dev
sudo pip install --upgrade pip 
sudo pip install --upgrade virtualenv 
sudo apt-get install -y libssl-dev libxml2-dev libxslt1-dev libssl-dev libffi-dev
sudo apt-get install python-swiftclient
sudo apt-get install unzip

#fix crazy keys
cat <<EOT >> .vimrc
:set nocompatible
set backspace=indent,eol,start
EOT

#edit bash
cat <<EOT >> .bashrc
#enron project's softlayer account
export ST_AUTH=https://dal05.objectstorage.softlayer.net/auth/v1.0/
export ST_USER=SLOS527663-3:arthurmak
export ST_KEY=f38a17790a83d57c5a3d1f2f56d5b957ee0fb206cf1db05d47eb77ff66085d7f
export SWIFT_AUTH_URL=https://dal05.objectstorage.softlayer.net/auth/v1.0/
export SWIFT_USER=SLOS527663-3:arthurmak
export SWIFT_KEY=f38a17790a83d57c5a3d1f2f56d5b957ee0fb206cf1db05d47eb77ff66085d7f
EOT

source ~/.bashrc