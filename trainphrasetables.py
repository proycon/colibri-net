#!/usr/bin/env python3

from __future__ import print_function, unicode_literals, division, absolute_import
import sys
import os
import glob
import datetime
import shutil
from multiprocessing import Pool

CORPUSDIR = "/vol/bigdata/corpora/OpenSubtitles2012/tokenized/"
EXPDIR = "/scratch/proycon/colibri-net/"

EXEC_MOSES_TRAINMODEL = '/vol/customopt/machine-translation/src/mosesdecoder/scripts/training/train-model.perl'
PATH_MOSES_EXTERNALBIN = '/vol/customopt/machine-translation/bin'

os.chdir(EXPDIR)

def process(data):
    num, lang,lang2 = data
    corpus1_orig = CORPUSDIR + "/OpenSubtitles2012." + lang + "-" + lang2 + '.' + lang + '.tok.gz'
    corpus2_orig = CORPUSDIR + "/OpenSubtitles2012." + lang + "-" + lang2 + '.' + lang2 + '.tok.gz'
    try:
        os.mkdir(EXPDIR + "/"+lang + "-" + lang2+'.work')
    except:
        pass
    corpus1 = EXPDIR + "/" + lang + "-" + lang2 + ".work/OpenSubtitles2012." + lang + "-" + lang2 + '.' + lang + '.tok.gz'
    corpus2 = EXPDIR + "/" + lang + "-" + lang2 + ".work/OpenSubtitles2012." + lang + "-" + lang2 + '.' + lang2 + '.tok.gz'
    if os.path.exists(corpus1_orig) and os.path.exists(corpus2_orig):
        print("Processing pair #" + str(num) + " -- " + lang + "-" + lang2 + " -- " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),file=sys.stderr)
        os.chdir(EXPDIR + "/"+ lang + "-" + lang2+'.work/')
        r = os.system("cp -f " + corpus1_orig + " " + corpus1)
        r = os.system("cp -f " + corpus2_orig + " " + corpus2)
        r = os.system("gunzip " + corpus1)
        r = os.system("gunzip " + corpus2)
        corpus1 = corpus1[:-3]
        corpus2 = corpus2[:-3]
        os.rename(corpus1, "corpus." + lang)
        os.rename(corpus2, "corpus." + lang2)


        r = os.system(EXEC_MOSES_TRAINMODEL + ' -external-bin-dir ' + PATH_MOSES_EXTERNALBIN + " -root-dir . --corpus corpus --f " + lang + " --e " + lang2 + ' --first-step 1 --last-step 8 >&2 2> train-model-' + lang + '-' + lang2 + '.log')
        os.rename("model/phrase-table.gz","../OpenSubtitles2012." + lang + "-" + lang2 + ".phrasetable.gz")
        if r:
            print("MOSES FAILED!",file=sys.stderr)
        else:
            os.chdir('..')
            shutil.rmtree(lang+'-'+lang2+'.work')

        print("Done " + lang + "-" + lang2 + " -- " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),file=sys.stderr)


def main():
    threads = int(sys.argv[1])

    print("Building inventory of available data files",file=sys.stderr)
    pairs = set()
    for filename in glob.glob(CORPUSDIR + '/*.tok.gz'):
        fields = os.path.basename(filename).split('.')
        L1 = fields[0].split('-')[0]
        L2 = fields[1].split('-')[0]
        pairs.add( (L1,L2) )

    pool = Pool(processes=threads)
    pairs = list(enumerate(pairs))
    print("Found ", len(pairs), " language pairs",file=sys.stderr)
    pool.map(process, pairs)      # prints "[0, 1, 4,..., 81]"




if __name__ == '__main__':
    main()




