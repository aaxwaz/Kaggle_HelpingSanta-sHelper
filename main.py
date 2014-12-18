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

'''
# init the Elfs DataFrame
Elfs = {'id' : [i+1 for i in range(numOfElfs)], 'avaiTime' : today + increment, 'productivity' : 1.0}
Elfs = pd.DataFrame(Elfs)
for i in range(numOfElfs):
    Elfs.iat[i,0] = datetime.utcfromtimestamp(Elfs.iat[i,0].astype(long) * (1e-9))'''

toys = pd.read_csv('toys_final.csv')  
# toys['Completed'] = pd.Series(int(0), index=toys.index)
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
    """ Elves are stored in a sorted list using heapq to maintain their order by next available time.
    List elements are a tuple of (next_available_time, elf object)
    :return: list of elves
    """
    list_elves = []
    for i in xrange(1, NUM_ELVES+1):
        elf = MyElf(today + increment, i, 1.0)
        heapq.heappush(list_elves, (elf.avaiTime, elf))
    return list_elves

Elfs = create_elves(numOfElfs)  # sorted by avaiTime
elfsReady = []                  # sorted by productivity
    
while (True):
    
    today = today + increment                                   # today is everyday at 9:00 am
    endOfToday = today + workingHours
    
    if (len(Elfs) + len(elfsReady) != numOfElfs):
        print "ERROR: Elfs and elfsReayd NOT total to %d!! \n\n\n" % numOfElfs
        sys.exit()
    
    while len(Elfs) > 0:
        avaiTime, tempElf = heapq.heappop(Elfs)
        if avaiTime > endOfToday:                     
            heapq.heappush(Elfs, (avaiTime, tempElf)) # sleeping ...
            break
        else:
            heapq.heappush(elfsReady, (tempElf.productivity, tempElf)) # ready to work 
    
    if len(elfsReady) == 0:
        print "%s : all elfs sleeping ... " % today
        continue
    print "%s : %d elfs starting to work ... " % (today, len(elfsReady)) 
    print "%f of toys has been completed!" % (finalResultIndex/10000000.0)
    
    while(toysIndex < totalToys):   # get ready all toys arrived by today
        if pd.Timestamp(toys.iat[toysIndex, 1]) <= today:
            toyInstance = MyToy(toys.iat[toysIndex,0], pd.Timestamp(toys.iat[toysIndex,1]), toys.iat[toysIndex,2])
            heapq.heappush(toysList, (toyInstance.duration, toyInstance))
            toysIndex += 1
        else:
            break
        
    # naive way to assign: small productivity elf do small duration job, big do big
    while ( len(elfsReady) > 0 ):       # while we have elfs available for today ... 
    
        if len(toysList) == 0:          # we have no ready toys
            if today > lastArrivalDay:  # all toys have been completed
                allToysCompleted = True
                break
            else:                       # ** To OPTIMIZE **
                print 'run out of toys for %s\n' % today
                break
        else:         
            thisElfProductivity, thisElf = heapq.heappop(elfsReady)
            while( len(toysList) > 0 ):
                thisDuration, thisToy = heapq.heappop(toysList)
                updatedProductivity, nextAvaiTime, actualWorkingMinutes = Task.ProcessTask(thisElf.avaiTime,thisElfProductivity,thisDuration, endOfToday) 
                
                # add this row to finalResult list
                tempResult = [thisToy.toyId, thisElf.id, thisElf.avaiTime, actualWorkingMinutes]
                finalResult.append(tempResult)
                finalResultIndex += 1
                
                # update this elf 
                if nextAvaiTime> endOfToday:   
                    thisElf.avaiTime = nextAvaiTime
                    thisElf.productivity = updatedProductivity
                    heapq.heappush(Elfs, (thisElf.avaiTime, thisElf))
                    break
                else:                                        # not put to sleep yet
                    thisElfProductivity = updatedProductivity
                    thisElf.avaiTime = nextAvaiTime
            # **** The first elf in elfsReady    
            
            elfsReadyRemained = len(elfsReady)
            if elfsReadyRemained == 0:
                print 'run out of elfs for %s' % today
                break
            
            # **** The last elf in elfsReady  
            heapq._heapify_max(elfsReady)
            thisElfProductivity, thisElf = heapq.heappop(elfsReady)
            heapq.heapify(elfsReady)
            while( len(toysList) > 0 ): 
                heapq._heapify_max(toysList)
                thisDuration, thisToy = heapq.heappop(toysList)
                heapq.heapify(toysList)
                updatedProductivity, nextAvaiTime, actualWorkingMinutes = Task.ProcessTask(thisElf.avaiTime,thisElfProductivity,thisDuration, endOfToday) 
                
                # add this row to finalResult list
                tempResult = [thisToy.toyId, thisElf.id, thisElf.avaiTime, actualWorkingMinutes]
                finalResult.append(tempResult)
                finalResultIndex += 1
                
                # update this elf  ## EXTREMELY SLOW!!! ##
                if nextAvaiTime > endOfToday:   
                    thisElf.avaiTime = nextAvaiTime
                    thisElf.productivity = updatedProductivity
                    heapq.heappush(Elfs, (thisElf.avaiTime, thisElf))
                    break
                else:                                  
                    thisElfProductivity = updatedProductivity #Productivity
                    thisElf.avaiTime = nextAvaiTime #avaiTime
            # **** The last elf in elfsReady  

            # update the last elf who has completed all the toys
            '''
            Elfs.loc[ Elfs.loc[:,'id'] == thisElf.id, 'productivity'] = updatedProductivity
            Elfs.loc[ Elfs.loc[:,'id'] == thisElf.id, 'avaiTime'] = nextAvaiTime'''
            if len(toysList) == 0 and thisElf.avaiTime < endOfToday:
                heapq.heappush(elfsReady, (thisElf.productivity, thisElf))            
            
        #else part  
    #while      

    if allToysCompleted == True:    # mission is done!!
        break
        
print "mission done!"

        
# end of Main while(True)
