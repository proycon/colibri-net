#!/usr/bin/env python3

from __future__ import print_function, unicode_literals, division, absolute_import
import os.path
import os
import sys
import datetime
from multiprocessing import Pool

#a subselection
langs = ['en','fr','es','nl','de','it','pt','ru','pl','sv','da','tr','el','fi','id']



CORPUSDIR = "/vol/bigdata/corpora/OpenSubtitles2012/"
EXPDIR = "/scratch/proycon/colibri-net/"

os.chdir(EXPDIR)




def getpairs():
    for lang in langs:
        for lang2 in langs:
            if lang < lang2:
                archivefile = CORPUSDIR + '/' + lang+"-"+lang2 + ".txt.zip"
                resultbase = CORPUSDIR + "/OpenSubtitles2012." + lang + "-" + lang2
                if os.path.exists(archivefile):
                    yield lang,lang2


threads = int(sys.argv[1])
if __name__ == '__main__':
    print(list(enumerate(getpairs())))


