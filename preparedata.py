#!/usr/bin/env python3

from __future__ import print_function, unicode_literals, division, absolute_import
import os.path
import os
import sys
import datetime
from multiprocessing import Pool

#a subselection
langs = ['en','fr','es','nl','de','it','pt','ru','pl','sv','da','tr','el','fi','id']


specifictokenisers = ['en','fr','es','nl','de','it']

CORPUSDIR = "/vol/bigdata/corpora/OpenSubtitles2012/"
EXPDIR = "/scratch/proycon/colibri-net/"

os.chdir(CORPUSDIR)


def tokenise(resultbase, lang):
    print("\tTokenising " + resultbase + '.' + lang,file=sys.stderr)
    if lang in specifictokenisers:
        r = os.system("ucto -d 1 -L"+lang + " -m -n " + resultbase + '.' + lang + ' > ' + resultbase + '.' + lang + '.tok 2> ' + resultbase + '.' + lang + '.log')
    else:
        r = os.system("ucto -d 1 -Lgeneric -m -n " + resultbase + '.' + lang + ' > ' + resultbase + '.' + lang + '.tok 2> ' + resultbase + '.' + lang + '.log' )
    if r:
        print("\tERROR: Tokeniser " + resultbase + '.' + lang + " returned with error!! Inspect log!",file=sys.stderr)



def process(data):
    num, lang,lang2 = data
    archivefile = CORPUSDIR + '/' + lang+"-"+lang2 + ".txt.zip"
    resultbase = CORPUSDIR + "/OpenSubtitles2012." + lang + "-" + lang2
    expresultbase = EXPDIR + "/OpenSubtitles2012." + lang + "-" + lang2
    print("Processing pair #" + str(num) + " -- " + lang + "-" + lang2 + " -- " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),file=sys.stderr)
    os.system("unzip " + archivefile)
    os.system("cp " + resultbase + "* " + EXPDIR)
    if not os.path.exists(expresultbase + '.' + lang + '.tok.gz'):
        tokenise(expresultbase, lang)
    if not os.path.exists(expresultbase + '.' + lang2 + '.tok.gz'):
        tokenise(expresultbase, lang2)
    #removing untokenised sources
    os.system("rm " + expresultbase + "." + lang)
    os.system("rm " + expresultbase + "." + lang2)
    #gzipping the rest
    os.system("gzip " + expresultbase + "." + lang + ".log")
    os.system("gzip " + expresultbase + "." + lang + ".tok")
    os.system("gzip " + expresultbase + "." + lang2 + ".log")
    os.system("gzip " + expresultbase + "." + lang2 + ".tok")
    print("Done " + lang + "-" + lang2 + " -- " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),file=sys.stderr)

def getpairs():
    num = 0
    for lang in langs:
        for lang2 in langs:
            if lang < lang2:
                archivefile = CORPUSDIR + '/' + lang+"-"+lang2 + ".txt.zip"
                resultbase = CORPUSDIR + "/OpenSubtitles2012." + lang + "-" + lang2
                if os.path.exists(archivefile) and (not os.path.exists(resultbase + '.' + lang+'.tok.gz') or not os.path.exists(resultbase + '.' + lang2+'.tok.gz')):
                    num += 1
                    yield num, lang,lang2


threads = int(sys.argv[1])
if __name__ == '__main__':
    pool = Pool(processes=threads)
    pairs = list(getpairs())
    print("Found ", len(pairs), " language pairs",file=sys.stderr)
    pool.map(process, pairs)      # prints "[0, 1, 4,..., 81]"


