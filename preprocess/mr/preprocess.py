#!/usr/bin/env python

from nltk import *
from nltk.corpus import stopwords
import sys

last_file = None
tokens_in_file = []

# NLTK
os.environ["NLTK_DATA"] = "/usr/share/nltk_data"
sw = stopwords.words('english')

#tokenizer = RegexpTokenizer(r'\w+|\$[\d\.]+|\S+')
tokenizer = RegexpTokenizer(r'\w+')
ls = LancasterStemmer()


def tokenize_stem(line):
    """Tokenize, Remove Stop Words, and Stem each word"""
    # Tokenize
    tokens = [t for t in tokenizer.tokenize(line) if t.lower() not in sw]
    # Stem
    tokens = [ls.stem(t) for t in tokens]

    return tokens


# input comes from STDIN (standard input)
for line in sys.stdin:
    try:
         # remove leading and trailing whitespace
        line = line.strip()
        # Tokenize & Stem
        tokens = tokenize_stem(line)
        # Keep accumulating tokens for same file
        tokens_in_file.extend(tokens)

        # Get current file name
        filename = os.environ['map_input_file']

        if last_file is not None and filename != last_file:
            if tokens_in_file and len(tokens_in_file) > 0:
                print " ".join(tokens_in_file)

            # Reset
            last_file = filename
            del tokens_in_file[:]

    except Exception as e:
        print e
        print("[Mapper] Ignoring record: {0}".format(line))
        continue