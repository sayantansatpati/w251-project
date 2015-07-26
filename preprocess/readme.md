#Provision server [CHANGE key name]
slcli vs create --hostname=preprocessor2 --domain=test.net --cpu 1 --memory 12288 -o UBUNTU_LATEST --datacenter=dal05 --billing=hourly --key atm

#SCP scripts and SSH into the newly provisioned server [CHANGE IP address]
scp setup.sh root@192.155.194.138:
scp clean.py root@192.155.194.138:
ssh root@192.155.194.138

#Once ssh inside the server, run below script to setup:
sh setup.sh

#test script
swift download w251-enron /enron/edrm-enron-v2/edrm-enron-v2_allen-p_pst.zip

#If download of file is successful, your server is ready to rock with swift!
#you may need to enter "source .bashrc" if it doesn't recognize your swift credentials

#Modify zipfile.py core python file such that it will allow unzipping files that span multiple disks 
vi /usr/lib/python2.7/zipfile.py
#commment the following lines:
if diskno != 0 or disks != 1:
    raise BadZipFile("zipfiles that span multiple disks are not supported")

#2 OPTIONS FOR YOUR TO RUN THE PYTHON SCRIPT
#Option 1: just clean the data and save everything in "/clean" folder in the server
python clean.py no-attach local 1 150

#Option 2: in additon to job in Option 1, also upload to SWIFT OBJECT STORAGE (much slower)
python clean.py no-attach swift 1 150

#Check whether files uploaded properly at Swift storage
swift list w251-enron


