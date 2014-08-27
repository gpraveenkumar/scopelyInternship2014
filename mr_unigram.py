# -*- coding: utf-8 -*-
"""
Created on Tue Apr  1 19:41:07 2014

@author: Praveen Kumar
"""

# The following parameter is used a set the minimum number of page likes 
# needs for it to be considered for bigrams.
Cutoff = 100

#Given a list of tokens, return a list of tuples of tokens and a count of '1'.
def Map(L):
 
  results = []
  for k in L:
      l = k.strip().split(',')
      for i in range(1,len(l)):
        #for j in range(i,len(l)):
        #  results.append ((l[i]+'-'+l[j], 1))
        
        results.append ((l[i], 1))
 
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

 
if __name__ == '__main__':
 
  f = open('data/features.csv')
  text = f.readlines()
  f.close()
  
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
  f = open('data/unigram.txt','w+')
  for pair in term_frequencies:
    #print(pair[0], " ", pair[1])
    f.write(pair[0]+ " "+ str(pair[1])+'\n')
  f.close()
  print("done.")