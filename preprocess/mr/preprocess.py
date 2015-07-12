#!/usr/bin/env python

from nltk import *
from nltk.corpus import stopwords
import sys

# NLTK
os.environ["NLTK_DATA"] = "~/nltk_data"
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

        if tokens and len(tokens) > 0:
            print " ".join(tokens)

    except Exception as e:
        print e
        print("[Mapper] Ignoring record: {0}".format(line))
        continue