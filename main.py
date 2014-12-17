

__author__ = 'Weimin'
__date__ = '17 December, 2014'

import pandas as pd
import time
import numpy as np
from datetime import datetime
from datetime import timedelta
import math
import gc
from Task import Task
from MyToy import MyToy
import heapq

numOfElfs = 900
prodUpper = 4.0
prodLower = 0.25
startTime = datetime(2014, 1, 1, 9, 0)
increment = timedelta(days=1)
workingHours = timedelta(hours=10.0)
today = startTime - increment
lastArrivalDay = datetime(2014, 12,10,23,59)

# init the Elfs DataFrame
Elfs = {'id' : [i+1 for i in range(numOfElfs)], 'avaiTime' : today + increment, 'productivity' : 1.0}
Elfs = pd.DataFrame(Elfs)

toys = pd.read_csv('toys_final.csv')  
# toys['Completed'] = pd.Series(int(0), index=toys.index)
toys.iloc[:,1] = toys.iloc[:,1].apply(lambda x: datetime.strptime(x, '%Y %m %d %H %M'))    

allToysCompleted = False
finalResultIndex = 0
 
# Create the finalResult dataframe
columns = ['ToyId', 'ElfId', 'StartTime', 'Duration']
i = range(1,10000001)
finalResult = pd.DataFrame(columns = columns,index = i)
# give proper datatype for each column
finalResult['ToyId'] = int(1)
finalResult['ElfId'] = int(1)
finalResult['StartTime'] = datetime(2014,1,1,0,1)
finalResult['Duration'] = int(1)

print 'finalResult DataFrame has been created\n'
#Declare: 
thisElf = Elfs.loc[0,:]

toysIndex = 0
totalToys = 10000000
toysList = []

while (True):
    
    today = today + increment                                   # today is everyday at 9:00 am
    endOfToday = today + workingHours
    elfsReady = Elfs.loc[ Elfs.loc[:,'avaiTime'] <= endOfToday ]
    if len(elfsReady) == 0:
        print "%s : all elfs sleeping ... " % today
        continue
    print "%s : %d elfs starting to work ... " % (today, len(elfsReady)) 
    print "%f of work toys has been completed! " % (finalResultIndex/10000000.0)
    elfsReady = elfsReady.sort('productivity')
    
    while(toysIndex < totalToys):
        if pd.Timestamp(toys.iat[toysIndex, 1]) <= today:
            toyInstance = MyToy(toys.iat[toysIndex,0], pd.Timestamp(toys.iat[toysIndex,1]), toys.iat[toysIndex,2])
            heapq.heappush(toysList, (toyInstance.duration, toyInstance))
            toysIndex += 1
        else:
            break
        
    # naive way to assign: small productivity elf do small duration job, big do big
    while ( len(elfsReady) > 0 ):       # while we have elfs available for today ... 
    
        if len(toysList) == 0:              # we have no ready toys
            if today > lastArrivalDay:  # all toys have been completed
                allToysCompleted = True
                break
            else:                       # ** To OPTIMIZE **
                print 'run out of toys for %s' % today
                break
        else:
            # **** The first elf in elfsReady
            thisElf.iat[0] = pd.Timestamp(elfsReady.iat[0,0]) # pick up the elf 
            thisElf.iat[1] = elfsReady.iat[0,1]
            thisElf.iat[2] = elfsReady.iat[0,2]
            while( len(toysList) > 0 ): # keep assigning to this Elf till he sleeps ...
                
                thisDuration, thisToy = heapq.heappop(toysList)
                
                updatedProductivity, nextAvaiTime, actualWorkingMinutes = Task.ProcessTask(thisElf.iat[0],thisElf.iat[2],thisDuration, endOfToday) # call the ProcessTask function
                
                # add this row to finalResult dataframe
                finalResult.iat[finalResultIndex,0] = thisToy.toyId # ToyId 
                finalResult.iat[finalResultIndex,1] = thisElf.iat[1] # ElfId
                finalResult.iat[finalResultIndex,2] = thisElf.iat[0] # StartTime             
                finalResult.iat[finalResultIndex,3] = actualWorkingMinutes 
                finalResultIndex += 1
                
                # update this elf 
                if nextAvaiTime> endOfToday:   # has put him to sleep, and jump to endIndex elf
                    #Update Elfs list for this elf
                    Elfs.loc[ Elfs.loc[:,'id'] == thisElf.at['id'], 'productivity'] = updatedProductivity
                    Elfs.loc[ Elfs.loc[:,'id'] == thisElf.at['id'], 'avaiTime'] = nextAvaiTime
                    elfsReady = elfsReady.loc[ elfsReady.loc[:,'id'] != thisElf.at['id'] ] # delete thisElf from elfsReady
                    break; 
                else:                                        # not put to sleep yet
                    thisElf.iat[2] = updatedProductivity
                    thisElf.iat[0] = nextAvaiTime
            # **** The first elf in elfsReady    
            
            elfsReadyRemained = len(elfsReady)
            if elfsReadyRemained == 0:
                print 'run out of elfs for %s' % today
                break
            
            # **** The last elf in elfsReady  
            thisElf.iat[0] = pd.Timestamp(elfsReady.iat[elfsReadyRemained-1,0]) # pick up the elf 
            thisElf.iat[1] = elfsReady.iat[elfsReadyRemained-1,1]
            thisElf.iat[2] = elfsReady.iat[elfsReadyRemained-1,2]
            
            while( len(toysList) > 0 ): # keep assigning to this Elf till he sleeps ...
                
                heapq._heapify_max(toysList)
                thisDuration, thisToy = heapq.heappop(toysList)
                heapq.heapify(toysList)
                
                updatedProductivity, nextAvaiTime, actualWorkingMinutes = Task.ProcessTask(thisElf.iat[0],thisElf.iat[2],thisDuration, endOfToday) # call the ProcessTask function
                
                # add this row to finalResult dataframe
                finalResult.iat[finalResultIndex,0] = thisToy.toyId # ToyId 
                finalResult.iat[finalResultIndex,1] = thisElf.iat[1] # ElfId
                finalResult.iat[finalResultIndex,2] = thisElf.iat[0] # StartTime             
                finalResult.iat[finalResultIndex,3] = actualWorkingMinutes
                finalResultIndex += 1
                
                # update this elf  ## EXTREMELY SLOW!!! ##
                if result[1] > endOfToday:   # has put him to sleep
                    #Update Elfs list for this elf
                    Elfs.loc[ Elfs.loc[:,'id'] == thisElf.at['id'], 'productivity'] = updatedProductivity
                    Elfs.loc[ Elfs.loc[:,'id'] == thisElf.at['id'], 'avaiTime'] = nextAvaiTime
                    elfsReady = elfsReady.loc[ elfsReady.loc[:,'id'] != thisElf.at['id'] ] # delete thisElf from elfsReady
                    break; 
                else:                                  # not put to sleep yet
                    thisElf.iat[2] = updatedProductivity #Productivity
                    thisElf.iat[0] = nextAvaiTime #avaiTime
            # **** The last elf in elfsReady  

            # update the last elf who has completed all the toys
            Elfs.loc[ Elfs.loc[:,'id'] == thisElf.at['id'], 'productivity'] = updatedProductivity
            Elfs.loc[ Elfs.loc[:,'id'] == thisElf.at['id'], 'avaiTime'] = nextAvaiTime    
            
        #else part  
    #while      

    if allToysCompleted == True:    # mission is done!!
        break
        
# end of Main while(True)
        

        

        
