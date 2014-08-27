# -*- coding: utf-8 -*-
"""
Created on Sun Apr 13 14:29:24 2014

@author: Praveen Kumar
"""

import numpy, scipy.sparse
from sparsesvd import sparsesvd
from scipy import *
from numpy import linalg
import sys


CutOff = 100

"""
#u = numpy.load('data/u.npy')
s = numpy.load('data/s.npy')
#v = numpy.load('data/v.npy')

t = numpy.diag([1,2])
d = linalg.inv(t)
"""

arg = sys.argv[1]
likes = arg.split('=')[1].split(',')
likes = ['"'+i+'"' for i in likes]
print(likes)

f = open('data/unigram_aboveCutoff.txt')
features = []
for l in f:
    t = l.split(' ')
    features.append( int(t[0]) )
f.close()
  
features.sort()

id_likes = {}  
f = open('data/id-likes_mapping.txt',encoding='latin-1')
for l in f:
    t = l.strip().split('---###@@@###---')
    id_likes[ t[1] ] = int(t[0])
f.close()

ql = []

for i in likes:
    t = id_likes[i]
    if t in features:
        ql.append(t)
        
print(ql)
  