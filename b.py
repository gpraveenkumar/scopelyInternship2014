# -*- coding: utf-8 -*-
"""
Created on Sat Apr 12 19:14:56 2014

@author: Praveen Kumar
"""

f = open('data/unigram_aboveCutoff.txt')
features = []
for l in f:
    t = l.split(' ')
    features.append( int(t[0]) )
f.close()

features.sort()
  
f = open('data/features.csv')
f1 = open('data/new_f.csv','w+')
#text = f.readlines()

for c,k in enumerate(f):
    l = k.strip().split(',')
    
    new_l = []
    new_l.append(l[0])
    """
    for i in range(1,len(l)):
        if int(l[i]) in features:
            new_l.append(l[i])
    """
        
    i = 1
    j = 0
    while i < len(l) and j < len(features):
        if features[j] == int(l[i]):
            new_l.append(l[i])
            i += 1
            j += 1
        elif features[j] > int(l[i]):
            i += 1
        else:
            j += 1
      
    # if the length of new_l is lesser than 3, it has only the user_id and 
    # one element. As there are less than 2 elements we can't generate high
    # frequency pairs. 
    if len(new_l) < 3:
        continue
    else:
        f1.write(','.join(new_l)+'\n')
          
    if c%100==0:
        print(c)

print(c)
f.close()
f.close()