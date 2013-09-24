#!/usr/bin/env python3

from __future__ import print_function, unicode_literals, division, absolute_import
import os.path
import os
import sys
from multiprocessing import Pool

#a subselection
langs = ['en','fr','es','nl','de','it','pt','ru','pl','sv','da','tr','el','fi','id']


specifictokenisers = ['en','fr','es','nl','de','it']

CORPUSDIR = "/vol/bigdata/corpora/OpenSubtitles2012/"

os.chdir(CORPUSDIR)


def tokenise(resultbase, lang):
    if lang in specifictokenisers:
        os.system("ucto -L"+lang + " -m -n " + resultbase + '.' + lang + ' > ' + resultbase + '.' + lang + '.tok')
    else:
        os.system("ucto -Lgeneric -m -n " + resultbase + '.' + lang + ' > ' + resultbase + '.' + lang + '.tok')


def process(data):
    lang,lang2 = data
    archivefile = CORPUSDIR + '/' + lang+"-"+lang2 + ".txt.zip"
    resultbase = CORPUSDIR + "/OpenSubtitles2012." + lang + "-" + lang2
    print("Processing " + lang + "-" + lang2,file=sys.stderr)
    os.system("unzip " + archivefile)
    tokenise(resultbase, lang)

def getpairs():
    for lang in langs:
        for lang2 in langs:
            if lang < lang2:
                archivefile = CORPUSDIR + '/' + lang+"-"+lang2 + ".txt.zip"
                resultbase = CORPUSDIR + "/OpenSubtitles2012." + lang + "-" + lang2
                if os.path.exists(archivefile) and not os.path.exists(resultbase + '.' + lang+'.tok') and not os.path.exists(resultbase + '.' + lang2+'.tok'):
                    yield lang,lang2


threads = int(sys.argv[1])
if __name__ == '__main__':
    pool = Pool(processes=threads)        # start 4 worker processes
    pool.map(process, list(getpairs()))      # prints "[0, 1, 4,..., 81]"


