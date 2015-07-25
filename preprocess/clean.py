"""
PURPOSE OF THIS CLEANING SCRIPT
1. RUN ON SOFTLAYER INSTANCE TO PROCESS DATA (ZIP FILES) FROM SWIFT OBJECT STORE
2. GET RID OF USELESS EMAIL HEADER, FOOTER, AND USELESS REPETITIVE EMAIL FORMAT WORDS
3. RETURN ONE TEXT FILE PER EMAIL, ONE FOLDER PER USER
4. EACH TEXT FILE HAS ONE WORD PER LINE
"""
import os
import sys
import zipfile
import re
import time
import contextlib
import itertools
import swiftclient
import StringIO

#CONNECTION TO SWIFT
authurl = os.environ['SWIFT_AUTH_URL']
user = os.environ['SWIFT_USER']
key = os.environ['SWIFT_KEY']
# authurl = "https://dal05.objectstorage.softlayer.net/auth/v1.0/"
# user = "SLOS527663-3:arthurmak"
# key = "f38a17790a83d57c5a3d1f2f56d5b957ee0fb206cf1db05d47eb77ff66085d7f"
conn = swiftclient.client.Connection(authurl=authurl, user=user, key=key)

container_name = "w251-enron"

#FILE PATHS
ENRON_PATH = "./"
EMAIL_PATH = "enron/edrm-enron-v2/"
CLEAN_PATH = "clean/"

#REAL EXPRESSION PATTERNS AT FOLDER/FILENAME LEVEL
zip_p = re.compile(".*xml\.zip") #relevant zip files
textfile_all_p = re.compile("text_000.*txt") #all email textfiles
textfile_no_attach_p = re.compile("text_000.*\D{1,1}\.txt") #email textfiles without attachment ones
textfile_only_attach_p = re.compile("text_000.*\d{1,1}\.txt") #only attachment textfiles

#REAL EXPRESSION PATTERNS WITHIN FILE 
#(reference: http://rforwork.info/2013/11/04/enron-email-corpus-topic-model-analysis-part-2-this-time-with-better-regex/)
# email_pat = ".+@.+"
# to_pat = "To:.+\n"
to_pat = "To:"
# cc_pat = "cc:.+\n"
cc_pat = "cc:"
# subject_pat = "Subject:.+\n"
subject_pat = "Subject:"
# from_pat = "From:.+\n"
from_pat = "From:"
# sent_pat = "Sent:.+\n"
sent_pat = "Sent:"
# received_pat = "Received:.+\n"
received_pat = "Received:"
# ctype_pat = "Content-Type:.+\n"
ctype_pat = "Content-Type:"
# reply_pat = "Reply- Organization:.+\n"
reply_pat = "Reply- Organization:"
date_pat = "Date:.+\n"
xmail_pat = "X-Mailer:.+\n"
mimver_pat = "MIME-Version:.+\n"
xdoc_pat = "X-SDOC:.+\n"
xzlid_pat = "X-ZLID:.+\n"

dash_pat = "--+.+--+"
star_pat = "\*\*+.+\*\*+"
uscore_pat = " __+.+__+"
equals_pat = "==+.+==+"

pattern1 = re.compile(r''
  # +email_pat+'|'
  +to_pat+'|'
  +cc_pat+'|'
  +subject_pat+'|'
  +from_pat+'|'
  +sent_pat+'|'
  +received_pat+'|'
  +ctype_pat+'|'
  +reply_pat+'|'
  +date_pat+'|'
  +xmail_pat+'|'
  +mimver_pat+'|'
  +xdoc_pat+'|'
  +xzlid_pat
)

pattern2 = re.compile(r''
  +dash_pat+'|'
  +star_pat+'|'
  +uscore_pat+'|'
  +equals_pat, re.DOTALL
)

            # pattern = re.compile(r'(?:ttp_ws_sm|ttpv1)_(\d+)_')


#INPUTS / MODE SELECTION
#ARGUMENT 1: Options to deal with attachments (all textfiles, email textfiles w/o attachment textfiles, attachment textfiles only)
TEXTFILE_TYPE = sys.argv[1].lower() #choose between "all", "no-attach","attach"
if TEXTFILE_TYPE == "all":
  textfile_p = re.compile("text_000.*txt") #all textfiles
elif TEXTFILE_TYPE == "no-attach":
  textfile_p = re.compile("text_000.*\D{1,1}\.txt") #email textfiles w/0 attachment textfiles
elif TEXTFILE_TYPE == "no-attach":
  textfile_p = re.compile("text_000.*\d{1,1}\.txt") #only attachment textfiles
else:
  raise ValueError("Incorrect argument for textfile type input (only 'all','no-attach','attach' are allowed.")
#ARGUMENT 2: Options to upload to swift storage or not (type swift or local)
UPLOAD_TYPE = sys.argv[2].lower() == "swift"
#ARGUMENT 3: Option to select start folder index (1 to 150)
START_FOLDER = int(sys.argv[3])
#ARGUMENT 4: Option to select end folder index (1 to 150)
END_FOLDER = int(sys.argv[4])

#COUNTERS / TRACKERS
count_sender = 0  #count of senders being processed
num_tf = 0 #number of textfiles for a given sender
count_tf = 0 #count of textfiles being processed for a given sender
count_tf_total = 0 #count of textfiles being processed in all
percent_tf = 0.0 #percentage of textfiles being processed
count_upload = 0 #count of uploads to Swift Object Store
start_time = time.time() #track time

#loop through all email folders and find relevant sender's zip files
# for sender_folder in os.listdir(EMAIL_PATH):  #FOR LOCAL USE
for data in conn.get_container(container_name)[1]:
  sender_folder = data['name']
  if zip_p.match(sender_folder):
    count_sender +=1
    #only process folder within the correct folder index specified by user input
    if count_sender >= START_FOLDER and count_sender<= END_FOLDER:
      print "Processing Sender",count_sender
      #generate sender's directory in "/clean" directory if not already exist
      SENDER_PATH = sender_folder[35:-8]+"_"+str(count_sender)+"/"
      if not os.path.exists(os.path.dirname(CLEAN_PATH+SENDER_PATH)):
        os.makedirs(os.path.dirname(CLEAN_PATH+SENDER_PATH))
      #open the relevant sender's zip file
      #with contextlib.closing(zipfile.ZipFile(EMAIL_PATH+sender_folder)) as z: #FOR LOCAL SYSTEM
      zip_file_obj = conn.get_object(container=container_name, obj=sender_folder)
      with contextlib.closing(zipfile.ZipFile(StringIO.StringIO(zip_file_obj[1]))) as z:
        namelist = z.namelist()
        num_tf = len(namelist)
        count_tf = 0
        #loop through all files in zip file and open only relevant textfile
        for textfile in namelist:
          #report every 1000 text files of progress
          count_tf +=1
          count_tf_total +=1
          if count_tf%1000 == 0 or count_tf>=num_tf:
            percent_tf = count_tf/float(num_tf)
            print "Processed","{0:.0f}%".format(percent_tf*100),"of textfiles", 
            print "for Sender",count_sender,"(",
            print count_tf,"out of",num_tf,"files)"

          if textfile_p.match(textfile):
            with z.open(textfile) as tf:
            ### NEED TO FIX ERROR WITH SENDER 59 ###
            # zipfile.BadZipfile: zipfiles that span multiple disks are not supported
            ### END FIX ###

              #put textfile in memory and split it into list of words
              email=tf.read()

              email = pattern1.sub('', email)
              email = pattern2.sub('', email)
              wordlist=email.split()



              #FOR SEEING YOUR SAMPLE EMAIL TEXT
              #print email 
               
              #Clean and write word into new textfile
              filename = CLEAN_PATH+SENDER_PATH+textfile[9:]
              with open(filename,"w") as pf:

                  ### WRITE SCRIPT FOR COMBINING ATTACHMENT TEXT ###

                  ### ENDING SCRIPT FOR COMBINIGN ATTACHMENT TEXT ###


                for word in wordlist:
                  pf.write(word+"\n")

                # pf.write(email)

              #put file into object storage
              if UPLOAD_TYPE:
                with open(filename,"r") as pf:
                  conn.put_object(container='w251-enron', obj=filename, contents=pf, chunk_size=512000)


      print "Time lapse:",time.time()-start_time,"sec"
      print "Number of files processed:",count_tf_total
      print "Number of files uploaded:",count_upload
      print "-------------------------------------------"



