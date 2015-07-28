#!/usr/bin/env python

from nltk import *
from nltk.corpus import stopwords
import re
import os
import sys


last_file = None
tokens_in_file = []

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
    if tokens_in_file and len(tokens_in_file) > 0:
                print " ".join(tokens_in_file)


# input comes from STDIN (standard input)
for line in sys.stdin:
    try:
        if pattern.match(line):
            print("[Mapper][Filter] Ignoring record: {0}".format(line))
            continue

        # remove leading and trailing whitespace
        line = line.strip()
        # Tokenize & Stem
        tokens = tokenize_stem(line)
        # Keep accumulating tokens for same file
        tokens_in_file.extend(tokens)

        # Get current file name
        filename = os.environ['map_input_file']

        # First time
        if not last_file:
            last_file = filename

        if filename != last_file:
            write_tokens()
            # Reset
            last_file = filename
            del tokens_in_file[:]

    except Exception as e:
        print e
        print("[Mapper][Exception] Ignoring record: {0}".format(line))
        continue

# Remaining ones
write_tokens()