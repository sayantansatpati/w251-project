#!/usr/bin/env python

from nltk.tokenize.regexp import RegexpTokenizer
# from nltk.stem import WordNetLemmatizer
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
import os
import sys

# NLTK
os.environ["NLTK_DATA"] = "~/nltk_data"
sw = stopwords.words('english')

tokenizer = RegexpTokenizer(r'[a-zA-Z]+')
#wl = WordNetLemmatizer()
sn = SnowballStemmer("english")

# store tokens in a list. helps remove unwanted header data
buf = []


def tokenize_stem(line):
    """Tokenize, Remove Stop Words, and Lemmatize each word"""
    # Tokenize
    tokens = [t for t in tokenizer.tokenize(line) if t.lower() not in sw]
    # Stem
    #tokens = [wl.lemmatize(t) for t in tokens]
    tokens = [sn.stem(t) for t in tokens]

    return tokens

def flushBuffer(words):
    for token in words:
        print token


# input comes from STDIN (standard input)
for line in sys.stdin:
    try:
        # dump contents of buffer if patterns common to headers are present
        if 'x-sdoc' in line or 'x-zlid' in line or '-----' in line:
            del buf[:]

        # < and > tend to surround URLs and email addresses. ignore them
        if '<' in line:
            continue

        # end processing if separator found. we don't want to process footer
        if '*******' in line:
            break

        # remove leading and trailing whitespace
        line = line.strip()
        # Tokenize & Stem
        tokens = tokenize_stem(line)

        if tokens and len(tokens) > 0:
            # print " ".join(tokens)
            buf += tokens

        # print buffer contents and delete when it gets long enough
        if len(buf) > 100:
            flushBuffer(buf)
            del buf[:]

    except Exception as e:
        print e
        print("[Mapper] Ignoring record: {0}".format(line))
        continue

# print leftover tokens in buffer
if len(buf) > 0:
    flushBuffer(buf)
