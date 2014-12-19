''' My simple solution to Kaggle competition - helping santa's helpers'''
__author__ = 'weimin wang'
__date__ = 'December 19, 2015'

import pandas as pd
import time
import numpy as np
from datetime import datetime
from datetime import timedelta
import math
import gc
from Task import Task
from MyToy import MyToy
from MyElf import MyElf
import heapq

numOfElfs = 900
prodUpper = 4.0
prodLower = 0.25
startTime = datetime(2014, 1, 1, 9, 0)
increment = timedelta(days=1)
workingHours = timedelta(hours=10.0)
today = startTime - increment
lastArrivalDay = datetime(2014, 12,10,23,59)

toys = pd.read_csv('toys_final.csv')  
toys.iloc[:,1] = toys.iloc[:,1].apply(lambda x: pd.Timestamp(datetime.strptime(x, '%Y %m %d %H %M')))    

allToysCompleted = False
finalResultIndex = 0
 
# Create the finalResult dataframe
columnNames = ['ToyId', 'ElfId', 'StartTime', 'Duration']
finalResult = []

toysIndex = 0
totalToys = 10000000
toysList = []

def create_elves(NUM_ELVES):
    list_elves = []
    for i in xrange(1, NUM_ELVES+1):
        elf = MyElf(today + increment, i, 1.0)
        heapq.heappush(list_elves, (elf.avaiTime, elf))
    return list_elves

Elfs = create_elves(numOfElfs)  # sorted by avaiTime
elfsReady = []                  # elfs ready to work for today
    
while (True):                   # every day
    
    today = today + increment                                   # today is everyday at 9:00 am
    endOfToday = today + workingHours
    
    if (len(Elfs) + len(elfsReady) != numOfElfs):               # double check we didn't miss out any elfs
        print "ERROR: Elfs and elfsReady NOT total to %d!! \n\n\n" % numOfElfs
        sys.exit()
    
    while len(Elfs) > 0:
        avaiTime, tempElf = heapq.heappop(Elfs)
        if avaiTime > endOfToday:                                      # we have reached the elf whose avaiTime is later than today
            heapq.heappush(Elfs, (avaiTime, tempElf))            
            break
        else:
            heapq.heappush(elfsReady, (tempElf.productivity, tempElf)) # push to elfsReady: ready to work 
    
    if len(elfsReady) == 0:
        print "%s : all elfs sleeping ... " % today
        continue
    #print "%s : %d elfs starting to work ... " % (today, len(elfsReady)) 
    #print "%f of toys has been completed!" % (finalResultIndex/10000000.0)
    
    while(toysIndex < totalToys):   # get ready all toys arrived BEFORE today AND not yet assigned out
        if pd.Timestamp(toys.iat[toysIndex, 1]) <= today:
            toyInstance = MyToy(toys.iat[toysIndex,0], pd.Timestamp(toys.iat[toysIndex,1]), toys.iat[toysIndex,2])
            heapq.heappush(toysList, (toyInstance.duration, toyInstance))
            toysIndex += 1
        else:
            break
    
    # naive way to assign: small productivity elf do small duration job, big do big
    while ( len(elfsReady) > 0 ):       # while we have elfs available for today ... 
        if len(toysList) == 0:          # we have no ready toys
            if today > lastArrivalDay:  # all toys have been completed, mission completed!
                allToysCompleted = True
                break
            else:                      
                #print 'Completed all the toys for %s\n\n\n' % today
                break
        else:         
            thisElfProductivity, thisElf = heapq.heappop(elfsReady)
            while( len(toysList) > 0 ):
                thisDuration, thisToy = heapq.heappop(toysList)
                
                updatedProductivity, nextAvaiTime, actualWorkingMinutes = Task.ProcessTask(thisElf.avaiTime,thisElfProductivity,thisDuration, endOfToday) 
                # write to finalResult list 
                tempResult = [thisToy.toyId, thisElf.id, thisElf.avaiTime.strftime('%Y %m %d %H %M'), actualWorkingMinutes]
                finalResult.append(tempResult)
                finalResultIndex += 1
                
                if nextAvaiTime> endOfToday:   
                    thisElf.avaiTime = nextAvaiTime
                    thisElf.productivity = updatedProductivity
                    heapq.heappush(Elfs, (thisElf.avaiTime, thisElf))
                    break
                else:                                        
                    thisElfProductivity = updatedProductivity
                    thisElf.avaiTime = nextAvaiTime
            # push back thisElf who has completed all the toys
            if len(toysList) == 0 and thisElf.avaiTime < endOfToday:
                heapq.heappush(elfsReady, (thisElf.productivity, thisElf))  
    if allToysCompleted == True:    # mission is done!!
        break
print "mission done!"

'''
# prepare submission file
resultFile = open('weiminSubmission.csv', 'wb')
wr = csv.writer(resultFile, dialect='excel')
wr.writerow(['ToyId', 'ElfId', 'StartTime', 'Duration'])
wr.writerows(finalResult)
resultFile.close()'''

# end of Main while(True)
