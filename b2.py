# -*- coding: utf-8 -*-
"""
Created on Tue Apr  1 19:41:07 2014

@author: Praveen Kumar
"""



# The following parameter is used a set the minimum number of likes for 
#high-frequency pairs.
Cutoff = 1000

from collections import Counter

#Given a list of tokens, return a list of tuples of tokens and a count of '1'.
def Map(L):

  f = open('data/unigram_above_Cutoff1.txt')
  features = []
  for l in f:
      t = l.split(' ')
      features.append( int(t[0]) )
  f.close()
  
  features.sort()
 
  results = []
  cnt = Counter()
  for k in L:
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
      
      l = new_l
      
      for i in range(1,len(l)):
        for j in range(i+1,len(l)):
          #results.append ((l[i]+'-'+l[j], 1))
          cnt [l[i]+'-'+l[j]] += 1
        
        #results.append ((l[i], 1))
        
      
  f5 = open('data/bi_g.txt','a')
  #print(cnt)
  for i in cnt:
    f5.write(i+' '+str(cnt[i])+'\n')
  f5.close()
  results.clear()
 
  #for i in cnt:
  #    results.append((i,cnt[i]))
 
  return results
 
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

#A generator function for chopping up a given list into chunks of length n.
def chunks(l, n):
  for i in range(0, len(l), n):
    yield l[i:i+n]
 
"""
Sort tuples by term frequency, and then alphabetically.
"""
def cmp(a, b):
    return (a > b) - (a < b)

def tuple_sort (a, b):
  if a[1] < b[1]:
    return 1
  elif a[1] > b[1]:
    return -1
  else:
    return cmp(a[0], b[0])
 
if __name__ == '__main__':
  
  f = open('data/features.csv')
  text = f.readlines()
  f.close()  
    
  f = open('data/unigram.txt')
  f1 = open('data/unigram_above_Cutoff1.txt','w')
  for l in f:
      t = l.split(' ')
      if int(t[1]) < Cutoff:
          break
      f1.write(l)
  
  f.close()  
  f1.close()      
  
  noOfProcessors = 25
  print("No of Processors:",noOfProcessors)
  
  # Build a pool of processes
  pool = Pool(processes=noOfProcessors,)
 
  # Fragment the string data into chunks
  partitioned_text = list(chunks(text, int(len(text) / 2000 )))
  #print(partitioned_text) 

  f5 = open('data/bi_g.txt','w')
  f5.close()
 
  # Generate count tuples for title-cased tokens
  print("Generating Tuples....")
  single_count_tuples = pool.map(Map, partitioned_text)
  print("done.")
     
  f = open('data/bi_g.txt')

  #c1=0
  c = Counter()
  for i,l in enumerate(f):
    
    t = l.strip().split(' ')
    c[t[0]] += int(t[1])    
    
    if i%10000000 == 0:
        print(i)
        #if c1 == 1:        
        #    break
        #c1 += 1

  f.close()
  print(i)
#print(c)

  f = open('data/bigram.txt','w')
  f1 = open('data/bigram_aboveCutoff.txt','w')
  for i in c:
    f.write(i+' '+str(c[i])+'\n')
    if c[i] >= Cutoff:
        f1.write(i+' '+str(c[i])+'\n')
        
  f.close()
  f1.close()
  
  """
  print("reading bi_counts...")
  f = open('data/bi_counts.txt')
  t = f.readlines()
  f.close()  
  single_count_tuples = list(chunks(text, int(len(text) / noOfProcessors)))
  print("done.")  
  
  # Organize the count tuples; lists of tuples by token key
  print("from tuples to list....")
  token_to_tuples = Partition(single_count_tuples)
  print("done.")
  
  # Collapse the lists of tuples into total term frequencies
  print("Getting frequency counts...")  
  term_frequencies = pool.map(Reduce, token_to_tuples.items())
  print("done.")
  
  # Sort the term frequencies in nonincreasing order
  term_frequencies.sort (key = itemgetter(0))
  term_frequencies.sort (key = itemgetter(1), reverse=True)
 
  print("Writing to file...")
  f = open('data/bigram.txt','w+')
  for pair in term_frequencies:
    #print(pair[0], " ", pair[1])
    f.write(pair[0]+ " "+ str(pair[1])+'\n')
  f.close()
  f = open('data/bigram_aboveCutoff.txt','w+')
  for pair in term_frequencies:
    if pair[1] < Cutoff:
        break
    f.write(pair[0]+ " "+ str(pair[1])+'\n')
  f.close()
  print("done.")
  """