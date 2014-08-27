# -*- coding: utf-8 -*-
"""
Created on Sun Apr 20 17:58:05 2014

@author: Praveen Kumar
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Apr  1 19:41:07 2014

@author: Praveen Kumar
"""



# The following parameter is used a set the minimum number of likes for 
#high-frequency pairs.
Cutoff = 100

from collections import Counter

#Given a list of tokens, return a list of tuples of tokens and a count of '1'.
def Map(L):

    print(L)  

#A generator function for chopping up a given list into chunks of length n.
def chunks(l, n):
  for i in range(0, len(l), n):
    yield l[i:i+n]
 

from multiprocessing import Pool
 
if __name__ == '__main__':
  
  text = list(range(100))  
  
  noOfProcessors = 4
  print("No of Processors:",noOfProcessors)
  
  # Build a pool of processes
  pool = Pool(processes=noOfProcessors,)
 
  # Fragment the string data into chunks
  partitioned_text = list(chunks(text, int(len(text) / 11 )))
  #print(partitioned_text) 

  #  f5 = open('data/bi_g.txt','w')
  #  f5.close()
 
  # Generate count tuples for title-cased tokens
  print("Generating Tuples....")
  single_count_tuples = pool.map(Map, partitioned_text)
  print("done.")
     
