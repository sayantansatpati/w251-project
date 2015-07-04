#PART 1 - GET AWS ENRON DATASET BY MOUNTING DATA ON EC2 INSTANCE

# creates a 215 GiB General Purpose (SSD) volume in the Availability Zone us-east-1b for the data we use for Enron
aws ec2 create-volume --snapshot snap-d203feb5 --size 215 --region us-east-1 --availability-zone us-east-1b --volume-type gp2

#created a new EC2 instance (OS = ubuntu 14.04 64-bit, disc = 8GB, micro instance)
# I used AWS console to perform this task

#attach volume to instance
aws ec2 attach-volume --volume-id vol-b032d05a --instance-id i-9a3d1233 --device /dev/sdf

#go to directory with the .pem key and ssh into AWS
chmod 400 w251project.pem
ssh -i w251project.pem ubuntu@54.175.219.70

#update system
sudo apt-get update
sudo apt-get install unzip

#Use the lsblk command to view your available disk devices and their mount points (if applicable) to help you determine the correct device name to use.
lsblk

#check whether file system exists (confirmed that file system already exists!)
sudo file -s /dev/xvdf

#create mount point
sudo mkdir /data
sudo mount /dev/xvdf /data

#check data size
sudo du -sh /data/*

#-------------------

#PART 2 - SETUP SWIFT STORAGE






#------------------------
#PROVISION SERVER
# slcli vs create --hostname=preprocessor --domain=test.net --cpu 1 --memory 1024 -o UBUNTU_LATEST --datacenter=dal05 --billing=hourly --key atm


slcli vs create --hostname=preprocessor2 --domain=test.net --cpu 1 --memory 12288 -o UBUNTU_LATEST --datacenter=dal05 --billing=hourly --key atm

#1024,2048,4096,6144,8192,12288,16384,32768,49152,65536     

#preprocessor1
ssh root@50.97.164.130

#preprocessor2
scp setup.sh root@192.155.194.138:
scp preprocess_v2.py root@192.155.194.138:
ssh root@192.155.194.138

sh setup.sh

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





#test script
swift download w251-enron /enron/edrm-enron-v2/edrm-enron-v2_allen-p_pst.zip

#perform actual work
python preprocess_v2.py no-attach





  scp load_data_am.py root@50.97.164.130:



swift -A https://dal05.objectstorage.softlayer.net/auth/v1.0/ -U SLOS527663-3:arthurmak -K f38a17790a83d57c5a3d1f2f56d5b957ee0fb206cf1db05d47eb77ff66085d7f list


Public: https://dal05.objectstorage.softlayer.net/auth/v1.0/
Private: https://dal05.objectstorage.service.networklayer.com/auth/v1.0/


swift list w251-enron



swift download w251-enron /enron/edrm-enron-v2/edrm-enron-v2_allen-p_pst.zip


swift download w251-enron /enron/edrm-enron-v2/edrm-enron-v2_allen-p_xml.zip

