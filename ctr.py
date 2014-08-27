# -*- coding: utf-8 -*-
"""
Created on Sat Apr 12 21:18:12 2014

@author: Praveen Kumar
"""

from collections import Counter

c = Counter()

f = open('data/bi_counts.txt')

c1=0
for i,l in enumerate(f):
    
    c[l.strip()] +=1    
    
    if i%100000 == 0:
        print(i)
        if c1 == 1:        
            break
        c1 += 1

f.close()
print(i)
#print(c)

f = open('data/bigram.txt','w')

for i in c:
    f.write(i+' '+str(c[i])+'\n')
    
f.close()