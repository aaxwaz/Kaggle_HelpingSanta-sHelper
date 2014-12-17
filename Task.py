import math
from datetime import datetime
from datetime import timedelta

class Task:

    # define main task processing engine
    @staticmethod
    def ProcessTask(thisElfAvaiTime, thisElfProd, thisToyDuration, endOfToday):
        # First, check to see if this toys is small enough to "fit in"
        actualWorkingMinutes = int(math.ceil(thisToyDuration/thisElfProd)) # int 
        actualStartTime = thisElfAvaiTime # datetime  int
        actualCompleteTime = actualStartTime + timedelta(minutes = actualWorkingMinutes)# datetime int
        if actualCompleteTime <= endOfToday:      # can fit in
            n = actualWorkingMinutes
            updatedProductivity = thisElfProd * (1.02**(n/60.0))
            updatedProductivity  = 4.0 if updatedProductivity > 4.0 else updatedProductivity
            nextAvaiTime = actualCompleteTime
            
            # return values: 
            result = [updatedProductivity, nextAvaiTime, actualWorkingMinutes]
            return result
        else:           #long working hours 
            numberOfDaysSpent = actualWorkingMinutes/float(24*60) # float
            numberOfWholeDays = math.floor(numberOfDaysSpent) # float
            numberOfFractionDay = numberOfDaysSpent - numberOfWholeDays # float
            workingMinutesOfToday = (endOfToday - thisElfAvaiTime).seconds/60.0 #float
            
            if actualCompleteTime.hour >= 9 and actualCompleteTime.hour < 19:  # situation 1: after next day 9am
                m = (actualCompleteTime.day - today.day) * 14.0 * 60.0 # float in minutes
                n = actualWorkingMinutes - m                           # float in minutes
                updatedProductivity = thisElfProd * (1.02**(n/60.0)) * (0.9**(m/60.0))
                updatedProductivity = 0.25 if updatedProductivity < 0.25 else updatedProductivity
                updatedProductivity = 4.0 if updatedProductivity > 4.0 else updatedProductivity
            else:   
                n = numberOfWholeDays * 10 * 60 + workingMinutesOfToday
                m = actualWorkingMinutes - n
                updatedProductivity = thisElfProd * (1.02**(n/60.0)) * (0.9**(m/60.0))
                updatedProductivity = 0.25 if updatedProductivity < 0.25 else updatedProductivity
                updatedProductivity = 4.0 if updatedProductivity > 4.0 else updatedProductivity
            
            # *** calculate nextAvaiTime 
            restDaysNeeded = m / (10.0 * 60.0) ## m is float in minutes(whole number)
            wholeRestDaysNeeded = math.floor(restDaysNeeded) # float
            remainingRestMinutes = timedelta(minutes=(m - wholeRestDaysNeeded * 60 * 10)) #float in minutes (whole number)
            endOfActualCompleteDay = datetime(actualCompleteTime.year, actualCompleteTime.month, actualCompleteTime.day, 19,0)
            if actualCompleteTime + remainingRestMinutes <= endOfActualCompleteDay:
                if actualCompleteTime.hour >= 9: 
                    nextAvaiTime = actualCompleteTime + remainingRestMinutes + timedelta(days = wholeRestDaysNeeded)
                else:
                    nextAvaiTime = datetime(actualCompleteTime.year, actualCompleteTime.month, actualCompleteTime.day, 9,0) + remainingRestMinutes + timedelta(days = wholeRestDaysNeeded)
            else:
                if actualCompleteTime.hour < 19:
                    nextAvaiTime = datetime(actualCompleteTime.year, actualCompleteTime.month, actualCompleteTime.day, 9,0) + (actualCompleteTime + remainingRestMinutes - endOfActualCompleteDay) + timedelta(days = wholeRestDaysNeeded + 1)
                else:
                    nextAvaiTime = datetime(actualCompleteTime.year, actualCompleteTime.month, actualCompleteTime.day, 9,0) + remainingRestMinutes + timedelta(days = wholeRestDaysNeeded + 1)
            # *** calculate nextAvaiTime 
            
            # return values: 
            return updatedProductivity, nextAvaiTime, actualWorkingMinutes
            
