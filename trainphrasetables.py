#!/usr/bin/env python3

from __future__ import print_function, unicode_literals, division, absolute_import
import sys
import os
import glob

CORPUSDIR = "/vol/bigdata/corpora/OpenSubtitles2012/"
EXPDIR = "/scratch/proycon/colibri-net/"

def main():
    print("Building inventory of available data files",file=sys.stderr)
    data = set()
    for filename in glob.glob(CORPUSDIR + '/*.tok.gz'):
        fields = os.path.basename(filename).split('.')
        L1 = fields[0].split('-')[0]
        L2 = fields[1].split('-')[0]
        data.add( (L1,L2) )


if __name__ == '__main__':
    main()




