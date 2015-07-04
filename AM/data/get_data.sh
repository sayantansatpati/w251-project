# creates a 215 GiB General Purpose (SSD) volume in the Availability Zone us-east-1b for the data we use for Enron
aws ec2 create-volume --snapshot snap-d203feb5 --size 215 --region us-east-1 --availability-zone us-east-1b --volume-type gp2

#created a new EC2 instance (OS = ubuntu 14.04 64-bit, disc = 8GB, micro instance)
# I used AWS console to perform this task

#attach volume to instance
aws ec2 attach-volume --volume-id vol-b032d05a --instance-id i-9a3d1233 --device /dev/sdf

#go to key folder
chmod 400 w251project.pem
ssh -i w251project.pem ubuntu@54.175.219.70

#Use the lsblk command to view your available disk devices and their mount points (if applicable) to help you determine the correct device name to use.
lsblk

#check whether file system exists (confirmed that file system already exists!)
sudo file -s /dev/xvdf

#create mount point
sudo mkdir /data
sudo mount /dev/xvdf /data

#check data size
sudo du -sh /data/*

