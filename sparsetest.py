# -*- coding: utf-8 -*-
"""
Created on Mon Apr  7 12:24:35 2014

@author: Praveen Kumar
"""

import numpy, scipy.sparse
from sparsesvd import sparsesvd
from scipy import *

r = []
c = []
data = []
f = open('data/featuresForSVD.csv')

for i,line in enumerate(f):
    l = line.strip().split(" ")
    r.append(l[0])
    c.append(l[1])
    if i%1000000==0:
        print(i)

print(i)

f.close()

"""
mat = numpy.random.rand(200, 100) # create a random matrix
smat = scipy.sparse.csc_matrix(mat) # convert to sparse CSC format
ut, s, vt = sparsesvd(smat, 100) # do SVD, asking for 100 factors
assert numpy.allclose(mat, numpy.dot(ut.T, numpy.dot(numpy.diag(s), vt)))
"""


#row = array([0,2,2,0,1,2])
#col = array([0,0,1,2,2,2])
#data = array([1,2,3,4,5,6])
smat = scipy.sparse.csc_matrix( ([1]*len(r),(r,c)))
ut, s, vt = sparsesvd(smat, 100) # do SVD, asking for 100 factors
#print(s)
numpy.save('data/u.npy',ut)
numpy.save('data/s.npy',s)
numpy.save('data/v.npy',vt)
#assert numpy.allclose(mat, numpy.dot(ut.T, numpy.dot(numpy.diag(s), vt)))
