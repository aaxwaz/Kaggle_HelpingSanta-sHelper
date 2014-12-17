
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
columnNames = ['ToyId', 'ElfId', 'StartTime', 'Duration']
finalResult = []

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
                print 'run out of toys for %s/n' % today
                break
        else:                                                                                # thisElf[0]: avaiTime, [1]id, [2]productivity
            # **** The first elf in elfsReady
            thisElfAvaiTime = pd.Timestamp(elfsReady.iat[0,0]) # pick up the elf 
            thisElfId = elfsReady.iat[0,1]
            thisElfProductivity = elfsReady.iat[0,2]
            while( len(toysList) > 0 ): # keep assigning to this Elf till he sleeps ...
                
                thisDuration, thisToy = heapq.heappop(toysList)
                
                updatedProductivity, nextAvaiTime, actualWorkingMinutes = Task.ProcessTask(thisElfAvaiTime,thisElfProductivity,thisDuration, endOfToday) 
                
                # add this row to finalResult list
                tempResult = [thisToy.toyId, thisElfId, thisElfAvaiTime, actualWorkingMinutes]
                finalResult.append(tempResult)
                finalResultIndex += 1
                
                # update this elf 
                if nextAvaiTime> endOfToday:   # has put him to sleep, and jump to endIndex elf
                    #Update Elfs list for this elf
                    Elfs.loc[ Elfs.loc[:,'id'] == thisElfId, 'productivity'] = updatedProductivity
                    Elfs.loc[ Elfs.loc[:,'id'] == thisElfId, 'avaiTime'] = nextAvaiTime
                    elfsReady = elfsReady.loc[ elfsReady.loc[:,'id'] != thisElfId ] # delete thisElf from elfsReady
                    break
                else:                                        # not put to sleep yet
                    thisElfProductivity = updatedProductivity
                    thisElfAvaiTime = nextAvaiTime
            # **** The first elf in elfsReady    
            
            elfsReadyRemained = len(elfsReady)
            if elfsReadyRemained == 0:
                print 'run out of elfs for %s' % today
                break
            
            # **** The last elf in elfsReady  
            thisElfAvaiTime = pd.Timestamp(elfsReady.iat[elfsReadyRemained-1,0]) # pick up the elf 
            thisElfId = elfsReady.iat[elfsReadyRemained-1,1]
            thisElfProductivity = elfsReady.iat[elfsReadyRemained-1,2]
            while( len(toysList) > 0 ): # keep assigning to this Elf till he sleeps ...
                
                heapq._heapify_max(toysList)
                thisDuration, thisToy = heapq.heappop(toysList)
                heapq.heapify(toysList)
                
                updatedProductivity, nextAvaiTime, actualWorkingMinutes = Task.ProcessTask(thisElfAvaiTime,thisElfProductivity,thisDuration, endOfToday) 
                
                # add this row to finalResult list
                tempResult = [thisToy.toyId, thisElfId, thisElfAvaiTime, actualWorkingMinutes]
                finalResult.append(tempResult)
                finalResultIndex += 1
                
                # update this elf  ## EXTREMELY SLOW!!! ##
                if nextAvaiTime > endOfToday:   # has put him to sleep
                    #Update Elfs list for this elf
                    Elfs.loc[ Elfs.loc[:,'id'] == thisElfId, 'productivity'] = updatedProductivity
                    Elfs.loc[ Elfs.loc[:,'id'] == thisElfId, 'avaiTime'] = nextAvaiTime
                    elfsReady = elfsReady.loc[ elfsReady.loc[:,'id'] != thisElfId ] # delete thisElf from elfsReady
                    break
                else:                                  # not put to sleep yet
                    thisElfProductivity = updatedProductivity #Productivity
                    thisElfAvaiTime = nextAvaiTime #avaiTime
            # **** The last elf in elfsReady  

            # update the last elf who has completed all the toys
            Elfs.loc[ Elfs.loc[:,'id'] == thisElfId, 'productivity'] = updatedProductivity
            Elfs.loc[ Elfs.loc[:,'id'] == thisElfId, 'avaiTime'] = nextAvaiTime    
            
        #else part  
    #while      

    if allToysCompleted == True:    # mission is done!!
        break
        
print "mission done!"

        
# end of Main while(True)
        

        

        
