#!/usr/bin/env python3

from __future__ import print_function, unicode_literals, division, absolute_import
import os.path
import sys

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


for lang in langs:
    for lang2 in langs:
        if lang < lang2:
            archivefile = CORPUSDIR + '/' + lang+"-"+lang2 + ".txt.zip"
            resultbase = CORPUSDIR + "/OpenSubtitles2012." + lang + "-" + lang2
            if os.path.exists(archivefile) and not os.path.exists(resultbase + '.' + lang+'.tok') and not os.path.exists(resultbase + '.' + lang2+'.tok'):
                print("Processing " + lang + "-" + lang2,file=sys.stderr)
                os.system("unzip " + archivefile)
                tokenise(resultbase, lang)




