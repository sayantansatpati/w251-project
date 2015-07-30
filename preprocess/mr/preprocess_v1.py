#!/usr/bin/env python

from nltk import *
from nltk.corpus import stopwords
import re
import os
import sys


last_filename = None
tokens_per_email = []

# NLTK
os.environ["NLTK_DATA"] = "/usr/share/nltk_data"
sw = stopwords.words('english')

#tokenizer = RegexpTokenizer(r'\w+|\$[\d\.]+|\S+')
#tokenizer = RegexpTokenizer(r'\w+')
tokenizer = RegexpTokenizer(r'\w+')
ls = LancasterStemmer()

# Filter
pattern = re.compile(r'\A[\W]')


def tokenize_stem(line):
    """Tokenize, Remove Stop Words, and Stem each word"""
    # Tokenize
    tokens = tokenizer.tokenize(line)
    # Stop Word Removal
    tokens = [t for t in tokens if t.lower() not in sw]
    # Stem
    tokens = [ls.stem(t) for t in tokens]

    return tokens


def write_tokens():
    if tokens_per_email and len(tokens_per_email) > 0:
            sys.stdout.write(" ".join(tokens_per_email).encode('utf-8') + "\n")


# input comes from STDIN (standard input)
# Multiple emails in a single file separated by a space
for line in sys.stdin:
    try:
        # Get current file name
        #filename = os.environ['map_input_file']
        filename = "xxx"

        # First time
        if not last_filename:
            last_filename = filename

        if line is None or line == '' or len(line) == 0 or filename != last_filename:
            write_tokens()
            # Reset
            last_filename = filename
            del tokens_per_email[:]

        line = line.decode('utf-8')

        if pattern.match(line):
            #print("[Mapper][Filter] Ignoring record: {0}".format(line))
            continue

        # remove leading and trailing whitespace
        line = line.strip()
        # Tokenize & Stem
        tokens = tokenize_stem(line)
        # Keep accumulating tokens for same file
        tokens_per_email.extend(tokens)

    except Exception as e:
        print e
        print("[Mapper][Exception] Ignoring record: {0}".format(line))
        continue

# Remaining ones
write_tokens()