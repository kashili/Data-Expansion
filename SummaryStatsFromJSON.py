
import glob
import pandas as pd
import sys
import os
import json
import numpy as np
import csv

class SummaryStatsFromJSON:

    def totalOSTrips(self,osDF,osType):
        count=0
        osDF = osDF[(osDF.os == osType)]
        count = osDF['trips'].sum()
        return count

    def totalOSDistance(self,osDF,osType):
        count=0
        osDF = osDF[(osDF.os == osType)]
        count = osDF['totalDistance'].sum()
        return count



    def averageDistance(self, osDF, osType=None):
        if osType is not None:
            osDF = osDF[(osDF.os == osType)]

        averageDistance =  osDF['averageDistance'].mean()
        return averageDistance

    def topNTrips(self,osDF,ub,osType=None):
        if osType is not None:
            osDF = osDF[(osDF.os == osType)]

        longTrips=frozenset(osDF['maxDistance'])
        longTrips=sorted(longTrips, reverse=True)
        return longTrips[0:ub]

    def totalOSUsers(self,osDF,osType):
        count = 0
        #print("Shape of osDF={0}".format(osDF.shape))
        osDF=osDF[(osDF.os==osType)]
        #print("Shape of df={0}".format(osDF.shape))
        osUsers = len(set(osDF.userId))
        return osUsers

    def printShapeOfAllUsers(self,dfByUser):
        print("==================================================================")
        for userId,df in dfByUser.items():
            print("Shape of user {0} is {1}".format(userId,df.shape))

    def sumOfSemiSeparated(self, x): #deprecated
        print(x)
        if x[-1] == ';': #in iOS, sadly strings are ending with semicolon too
            x = x[:-1]
        try:
            return sum(map(float, x.split(';')))
        except:
            print("Error throwing distance {0}".format(x))

    def  expandedMasterDF(self,megaDF):
        #megaDF['totalDistance'] = megaDF['distance'].apply(lambda x: self.sumOfSemiSeparated(x))
        #megaDF['maxDistance'] = megaDF['distance'].apply(lambda x: self.maxOfSemiSeparated(x))
        #megaDF['minDistance'] = megaDF['distance'].apply(lambda x: self.minOfSemiSeparated(x))
        megaDF['totalDistance'] = megaDF['distance'].apply(lambda x: sum(map(float, x.split(';'))))
        megaDF['maxDistance'] = megaDF['distance'].apply(lambda x: max(map(float, x.split(';'))))
        megaDF['minDistance'] = megaDF['distance'].apply(lambda x: min(map(float, x.split(';'))))
        megaDF['averageDistance'] = megaDF['totalDistance']/megaDF['trips']
        megaDF['totalScore'] =  megaDF['score'].apply(lambda x: sum(map(int, x.split(';'))))
        megaDF['averageScore'] = megaDF['totalScore'] / megaDF['trips']
        megaDF['date'] = pd.to_datetime(megaDF['date'])
        megaDF['month'] = megaDF['date'].dt.strftime('%b') # month
        return megaDF

    def commonUsers(self,megaDF):
        androidDF = megaDF[(megaDF.os == 'Android' )]
        iOSDF = megaDF[(megaDF.os == 'iOS')]
        overlap = list(frozenset(androidDF['userId']) & frozenset(iOSDF['userId']))
        return overlap

    def  expandedMasterDFOld(self,megaDF):
        # expand semi colon separated columns and add few new columns for easier processing
        for line in megaDF['distance']:
            try:
                distancelist = list(map(float, line.split(';')))
                megaDF['totalDistance']=sum(distancelist)
            #megaDF['totalDistance']=megaDF['distance'].apply(lambda x: sum(map(float, x.split(';'))))
            except:
              print("Error with line {0}".format(line))
        return megaDF

    def masterDF(self,dfByUser):
        megaDF=pd.DataFrame()
        for userId, df in dfByUser.items():
            megaDF=megaDF.append(df)
        print("Shape of MEGADF  is {0}".format(megaDF.shape))
        return megaDF

    def writeDFsToFile(self, megaDF,fn):
        megaDF.to_csv(fn, index=False)

    def printStats(self,dfByUser,megaDF,expandedMegaDF):
        #self.printShapeOfAllUsers(dfByUser)
        #=self.masterDF(dfByUser)
        osDF = expandedMegaDF[['userId', 'os','trips','totalDistance','maxDistance','minDistance','averageDistance','averageScore']]
        print("==================================================================")
        #print("      # of Users again = {0}".format(len(dfByUser.keys())))
        print(" SUMMARY STATISTICS:-")
        print("      # of Users  = {0}".format(len(frozenset(megaDF['userId']))))
        print("      # of Android Users = {0}".format(self.totalOSUsers(osDF,'Android')))
        print("      # of iOS Users = {0}".format(self.totalOSUsers(osDF,'iOS')))
        print("      common Users across Android & iOS= {0}".format(self.commonUsers(osDF)))

        print("      # of Trips = {0}".format(osDF['trips'].sum()))
        print("      # of Android Trips = {0}".format(self.totalOSTrips(osDF,'Android')))
        print("      # of iOS Trips = {0}".format(self.totalOSTrips(osDF,'iOS')))

        print("      # Distance = {0}".format(osDF['totalDistance'].sum()))
        print("      # Android Distance = {0}".format(self.totalOSDistance(osDF, 'Android')))
        print("      # iOS Distance = {0}".format(self.totalOSDistance(osDF, 'iOS')))

        print("      Average Trip Length = {0}".format(self.averageDistance(osDF)))
        print("      Average Android Trip Length = {0}".format(self.averageDistance(osDF,'Android')))
        print("      Average iOS Trip Length = {0}".format(self.averageDistance(osDF,'iOS')))

        print()
        print()
        print(" TOP 10:-")
        print("      Top 10 Trip Lengths = {0}".format(self.topNTrips(osDF, 10)))
        print("      Top 10 Android Trip Lengths = {0}".format(self.topNTrips(osDF, 10, 'Android')))
        print("      Top 10 iOS Trip Lengths = {0}".format(self.topNTrips(osDF, 10, 'iOS')))
        print("      Top 10 overall Trip Logging Drivers =     [*MASKED OUT*]")
        print("      Top 10 overall Distance Logging Drivers = [*MASKED OUT*]")

        scoredf=expandedMegaDF[['averageScore','date','os']]
        scoredf.index=scoredf['date']

        print()
        print()
        print(" SCORE CARD:-")
        print("      Global Score for Erie city = {0}".format(osDF['averageScore'].mean()))
        print("      AverageScore for Erie city by month {0}".format(scoredf.resample('M').mean()))

        tripsdf = expandedMegaDF[['trips', 'date', 'os']]
        tripsdf.index = tripsdf['date']
        print()
        print()
        print(" BY MONTH:-")
        print("      Average #Trips for Erie city by month {0}".format(tripsdf.resample('D').mean()))


        totalDistancedf = expandedMegaDF[['totalDistance', 'date', 'os']]
        totalDistancedf.index = totalDistancedf['date']
        print()
        print()
        print(" BY MONTH:-")
        print("      Average distance for Erie city by month {0}".format(totalDistancedf.resample('D').mean()))

        print("==================================================================")

    def loadAllJSONs(self,path='/Users/ka/data/**/*'):
        print("glob type is {0}".format(type(glob)))
        dfByUser={} #dict()
        for filename in glob.glob(path, recursive=True):
            if os.path.isdir(filename):
                userdf=self.readJSONFileIntoFrame(filename)
                dfByUser[filename]=userdf
        return dfByUser

    def validJSON(self,jdata,jsondatafile):
        datevalue = jdata.get("date", None)
        distancevalue = jdata.get("distance", None)
        osvalue = jdata.get("os")

        if datevalue is None:
            print("IGNORING THE FILE with missing datevalue{0}".format(jsondatafile))
            return False

        if distancevalue is None or \
                not distancevalue:  # TODO: WTF it is not working; we want to ignore the legacy files with NULLs, but it is not working
            print("IGNORING THE FILE  with missing distancevalue {0}".format(jsondatafile))
            return False

        osvalue = jdata.get("os")
        if osvalue != 'Android' and osvalue != 'iOS':
            print("IGNORING THE file with invalid OS {0} {1}".format(osvalue,jsondatafile))
            return False

        return  True

    def cleanupJSONData(self,jdata):
        distancevalue = jdata.get("distance", None)
        if distancevalue[-1] == ';':  # in iOS, sadly strings are ending with semicolon too
            print("TAKING OUT SEMICOLON OF {0}".format(distancevalue))
            distancevalue = distancevalue[:-1]
            jdata["distance"] = distancevalue

        scorevalue = jdata.get("score", None)
        if scorevalue[-1] == ';':  # in iOS, sadly strings are ending with semicolon too
           # print("TAKING OUT SEMICOLON OF {0}".format(scorevalue))
            scorevalue = scorevalue[:-1]
            jdata["score"] = scorevalue

        return jdata

     def readJSONFileIntoFrame(self,path):
        nFiles=0;
        suffix = '*'
        path=os.path.join(path,suffix)
        #print(path)
        userdf = pd.DataFrame()
        for jsondatafile in glob.glob(path):
            #print("processing..{0}".format(jsondatafile))
            nFiles=nFiles+1
            #jdata = pd.read_json(jsondata)

            with open(jsondatafile, 'r') as f:
                jdata = json.load(f)
                """
                datevalue=jdata.get("date",None)
                if  datevalue is None: #TODO: WTF it is not working; we want to ignore the legacy files with NULLs, but it is not working
                    print("IGNORING THE FILE {0}".format(jsondatafile))
                    continue

                distancevalue = jdata.get("distance", None)
                if  distancevalue is None or \
                        not distancevalue : #TODO: WTF it is not working; we want to ignore the legacy files with NULLs, but it is not working
                    print("distancevalue is null..IGNORING THE FILE {0}".format(jsondatafile))
                    continue
                elif  distancevalue[-1] == ';': #in iOS, sadly strings are ending with semicolon too
                    print("TAKING OUT SEMICOLON OF {0}".format(distancevalue))
                    distancevalue = distancevalue[:-1]
                    jdata["distance"]=distancevalue

                osvalue = jdata.get("os")
                if osvalue !='Android' and osvalue !='iOS':
                    print("IGNORING THE RECORD {0}".format(jdata))
                    continue
                """
                if(not self.validJSON(jdata,jsondatafile)):
                    continue

                jdata = self.cleanupJSONData(jdata)


                rowdf = pd.DataFrame([jdata])#, dtype=np.str)#jdata, index=[0])
                #userdf=pd.concat([rowdf,userdf], axis=1)
                #print(rowdf)
                userdf=userdf.append(rowdf)
                #print(userdf)
        print("{0}....{1} ".format(path,nFiles))
        #print(userdf.shape)
        return userdf

dfByUser=SummaryStatsFromJSON().loadAllJSONs()
megaDF=SummaryStatsFromJSON().masterDF(dfByUser)
expandedMegaDF=SummaryStatsFromJSON().expandedMasterDF(SummaryStatsFromJSON().masterDF(dfByUser))
SummaryStatsFromJSON().writeDFsToFile(megaDF,'cumulativedataset.csv')
print("Shape of expandedMegaDF  is {0}".format(expandedMegaDF.shape))
SummaryStatsFromJSON().writeDFsToFile(expandedMegaDF,'cumulativeexpandeddataset.csv')
SummaryStatsFromJSON().printStats(dfByUser,megaDF,expandedMegaDF)
print(megaDF['os'].value_counts())
