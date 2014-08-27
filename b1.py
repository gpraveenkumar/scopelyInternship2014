# -*- coding: utf-8 -*-
"""
Created on Sat Apr 12 19:31:48 2014

@author: Praveen Kumar
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Apr  1 19:41:07 2014

@author: Praveen Kumar
"""

# The following parameter is used a set the minimum number of likes for 
#high-frequency pairs.
Cutoff = 25

#Given a list of tokens, return a list of tuples of tokens and a count of '1'.
def Map(L):

  f = open('data/unigram_aboveCutoff.txt')
  features = []
  for l in f:
      t = l.split(' ')
      features.append( int(t[0]) )
  f.close()
  
  features.sort()
 
  results = []
  c1 = 0
  for c,k in enumerate(L):
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
          results.append(','.join(new_l))
          c1 += 1
      
      #if c%100==0:
      #    print('process id:', os.getpid(),' ',c)
      
  #print('process id:', os.getpid(),' ',c)
  f = open('data/combine.csv','a')
  f.write('\n'.join(results))
  f.close()
  
  return (c,c1)
 
"""
Group the sublists of (token, 1) pairs into a term-frequency-list
map, so that the Reduce operation later can work on sorted
term counts. The returned result is a dictionary with the structure
{token : [(token, 1), ...] .. }
"""
def Partition(L):
  tf = {}
  for sublist in L:
    for p in sublist:
      # Append the tuple to the list in the map
      try:
        tf[p[0]].append (p)
      except KeyError:
        tf[p[0]] = [p]
  return tf
 
"""
Given a (token, [(token, 1) ...]) tuple, collapse all the
count tuples from the Map operation into a single term frequency
number for this token, and return a final tuple (token, frequency).
"""
def Reduce(Mapping):
  return (Mapping[0], sum(pair[1] for pair in Mapping[1]))
  

from multiprocessing import Pool
from operator import itemgetter
import os

#A generator function for chopping up a given list into chunks of length n.
def chunks(l, n):
  for i in range(0, len(l), n):
    yield l[i:i+n]
 
 
if __name__ == '__main__':
  
  f = open('data/features.csv')
  text = f.readlines()
  f.close()  
  #text = text[:10000]  
  
  noOfProcessors = 25
  print("No of Processors:",noOfProcessors)
  
  # Build a pool of processes
  pool = Pool(processes=noOfProcessors,)
 
  # Fragment the string data into chunks
  partitioned_text = list(chunks(text, int(len(text) / noOfProcessors)))
  #print(partitioned_text) 
  
  # Generate count tuples for title-cased tokens
  print("Generating Tuples....")
  single_count_tuples = pool.map(Map, partitioned_text)
  print("done.")
  print(single_count_tuples)
  
  print(sum(pair[0]+1 for pair in single_count_tuples))
  print(sum(pair[1] for pair in single_count_tuples))
 
  