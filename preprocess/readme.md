#Provision server [CHANGE key name]
slcli vs create --hostname=preprocessor2 --domain=test.net --cpu 1 --memory 12288 -o UBUNTU_LATEST --datacenter=dal05 --billing=hourly --key atm

#SCP scripts and SSH into the newly provisioned server [CHANGE IP address]
scp setup.sh root@192.155.194.138:
scp preprocess_v2.py root@192.155.194.138:
ssh root@192.155.194.138

#Once ssh inside the server, run below script to setup:
sh setup.sh

#test script
swift download w251-enron /enron/edrm-enron-v2/edrm-enron-v2_allen-p_pst.zip

#If download of file is successful, your server is ready to rock with swift!
#you may need to enter "source .bashrc" if it doesn't recognize your swift credentials

#2 OPTIONS FOR YOUR TO RUN THE PYTHON SCRIPT
#Option 1: just clean the data and save everything in "/clean" folder in the server
python clean.py no-attach local

#Option 2: in additon to job in Option 1, also upload to SWIFT OBJECT STORAGE (much slower)
python clean.py no-attach swift

#Check whether files uploaded properly at Swift storage
swift list w251-enron

####################
#TO DO's 

1. Issue with unzipping sender 59. Need to fix this error:
zipfile.BadZipfile: zipfiles that span multiple disks are not supported

2. Upload speed is too freakin slow

3. Further clean the data by:
(i) stripping out portions of emails that are not useful (ie. header/footers)
(ii) use NLTK to reduce set of words (ie. stemming, stop words, etc.)

4. Combine attachment info and create a new set of text data

5. Indexing the textfile and attachments

6. Figure out how to delete folder inside object store

