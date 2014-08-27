# -*- coding: utf-8 -*-
"""
Created on Sun Mar 23 16:21:06 2014

@author: Praveen Kumar
"""

import re

try:
    f = open('data/likes.csv',encoding='latin-1')
    f2 = open('data/features.csv','w+')
    f4 = open('data/id-usersid_mapping.txt','w+')
    j = 0
    fno_pages = {}
    fno = 1
    userno = 1
    print("Processed (no. of lines) of likes.csv ...")
    for i in f:
        t = i.strip()
        flag = 0
        
        # Following piece of code is used to append multiple lines to t in the 
        # case where a like have a '\n' (as this would correspond to multiple
        # lines in likes.csv)        
        
        try:
            while not(t[-1]=='"'):
                t += next(f).strip()
        except Exception as er:
            print(er)
        
        # Regular expression to parse likes.csv 
        l = re.split('(?<=\"),(?=\")',t)
        
        try:
            f4.write(str(userno)+','+l[0]+'\n')
            feature = str(userno) + ','
            ids = []
            for k in range(1,len(l)):
                if l[k] in fno_pages:
                    ids.append(fno_pages[ l[k] ])
                else:
                    fno_pages[ l[k] ] = fno
                    ids.append(fno)
                    fno += 1
            
            # Storing the ids in a sorted fashion -- will save time for other 
            #processing (e.g. map reduce) later.
            idset = set(ids)
            ids = list(idset)
            ids.sort()
            for v in ids:
                feature += str(v) + ','
                #f3.write(str(userno)+','+str(v)+'\n')
            
            f2.write(feature[:-1]+'\n')
        except Exception as er:
            print('ERROR:',er)
            pass
            
        userno += 1
        j += 1
        if(j%10000==0):
            print(j)
            
    print(j)
    f.close()
    f2.close()    
    #f3.close()
    f4.close()
    
    # Storing likes in a increasing sorted order of its mapping-ids.
    fno_page = {}        
    for i in fno_pages:
        fno_page[fno_pages[i]] = i
    
    f1 = open('data/id-likes_mapping.txt','wb')
    for i in fno_page:
        t = str(i) + '---###@@@###---'
        f1.write(t.encode('latin-1'))
        f1.write(fno_page[i].encode('latin-1'))
        f1.write('\n'.encode('latin-1'))
    
    f1.close()
    
except Exception as er:
        print('ERROR:',er,j)
