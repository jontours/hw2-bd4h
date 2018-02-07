#!/usr/bin/env python

import sys
import random

from optparse import OptionParser

parser = OptionParser()
parser.add_option("-n", "--model-num", action="store", dest="n_model",
                  help="number of models to train", type="int")
parser.add_option("-r", "--sample-ratio", action="store", dest="ratio",
                  help="ratio to sample for each ensemble", type="float")

options, args = parser.parse_args(sys.argv)

random.seed(6505)

for line in sys.stdin:
    for i in range(options.n_model):
        key = random.randint(0, options.n_model - 1)
        value = line.strip()
        cutoff = options.n_model * options.ratio
        if key < cutoff:
            print("%d\t%s" % (i, value))



