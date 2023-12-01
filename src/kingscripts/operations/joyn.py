import requests
import json
import datetime as dt
import re
import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np
import copy
from kingscripts.operations.combocurve import *

"""
    
This script gets all the modifed daily allocated production over to the master JOYN dataframe in prep to merge    

"""


def getDailyAllocatedProduction(workingDataDirectory, joynUsername, joynPassword, daysToLookBack):

    load_dotenv()

    if daysToLookBack == 0 or type(daysToLookBack) != int:
        print("Error: daysToLookBack must be an integer greater than 0")
        return

    # Date values
    dateToday = dt.datetime.today()
    dateTomorrow = dateToday + dt.timedelta(days=1)
    dateToLookBack = dateToday - timedelta(days=daysToLookBack)
    dateTomorrowString = dateTomorrow.strftime("%Y-%m-%d")
    dateToLookBackString = dateToLookBack.strftime("%Y-%m-%d")

    # Read in Master Data
    masterAllocationData = pd.read_excel(
        workingDataDirectory + r"\master\masterWellAllocation.xlsx")
    masterJoynData = pd.read_excel(
        workingDataDirectory + r"\production\masterJoynData.xlsx")
    forecastedProductionData = pd.read_csv(
        workingDataDirectory + r"\production\forecastWellsFinal.csv")

    # Get id, API and Well Accounting Name from masterAllocationData
    joynIdList = masterAllocationData["JOYN Id"].tolist()
    apiNumberList = masterAllocationData["API"].tolist()
    wellAccountingNameList = masterAllocationData["Name in Accounting"].tolist(
    )

    # Functions
    def getForecast(apiNumber, date):

        indexList = [index for index, value in enumerate(
            forecastedProductionData["API 14"].to_list()) if value == apiNumber]

        # Gets the forecasted prodcution for the current date
        forecastedDateList = [forecastedProductionData["Date"][index]
                              for index in indexList]

        # finds the index of the current date in the forecasted data and sets to the current forecasted volume
        if date in forecastedDateList:
            forecastedIndex = forecastedDateList.index(date)
            oilVolumeForecast = forecastedProductionData["Oil"][indexList[forecastedIndex]]
            gasVolumeForecast = forecastedProductionData["Gas"][indexList[forecastedIndex]]
            waterVolumeForecast = forecastedProductionData["Water"][indexList[forecastedIndex]]
        else:
            oilVolumeForecast = 0
            gasVolumeForecast = 0
            waterVolumeForecast = 0

        return oilVolumeForecast, gasVolumeForecast, waterVolumeForecast

    # Function to split date from JOYN API into correct format - returns date in format of 5/17/2023 from format of 2023-05-17T00:00:00

    def splitDateFunction(badDate):
        splitDate = re.split("T", badDate)
        splitDate2 = re.split("-", splitDate[0])
        year = int(splitDate2[0])
        month = int(splitDate2[1])
        day = int(splitDate2[2])
        dateString = str(month) + "/" + str(day) + "/" + str(year)

        return dateString

    # Function to authenticate JOYN API - returns idToke used as header for authorization in other API calls

    def getIdToken():

        load_dotenv()

        login = joynUsername
        password = joynPassword

        # User Token API
        url = "https://api.joyn.ai/common/user/token"
        # Payload for API - use JOYN crdentials
        payload = {
            "uname": str(login),
            "pwd": str(password)
        }
        # Headers for API - make sure to use content type of json
        headers = {
            "Content-Type": "application/json"
        }

        # dump payload into json format for correct format
        payloadJson = json.dumps(payload)
        response = requests.request(
            "POST", url, data=payloadJson, headers=headers)  # make request
        if response.status_code == 200:  # 200 = success
            print("Successful JOYN Authentication")
        else:
            print(response.status_code)

        results = response.json()  # get response in json format
        idToken = results["IdToken"]  # get idToken from response

        return idToken

    # Function to change product type to Oil, Gas, or Water

    def switchProductType(product):
        if product == 760010:
            product = "Gas"
        elif product == 760011:
            product = "Oil"
        elif product == 760012:
            product = "Water"
        else:
            product = "Unknown"

        return product

    # Function to get API number from JOYN ID using masterAllocationSheet

    def getApiNumber(uuid):
        if uuid in joynIdList:
            index = joynIdList.index(uuid)
            apiNumberYay = apiNumberList[index]
        else:
            apiNumberYay = "Unknown"

        return apiNumberYay

    # Function to get Well Accounting Name from JOYN ID using masterAllocationSheet
    def getName(apiNumber2):
        if apiNumber2 in apiNumberList:
            index = apiNumberList.index(apiNumber2)
            name = wellAccountingNameList[index]
        else:
            name = "Unknown"

        return name

    # Function to get State from JOYN ID using masterAllocationSheet

    def getState(apiNumber):
        if apiNumber in apiNumberList:
            index = apiNumberList.index(apiNumber)
            state = masterAllocationData["State"][index]
        else:
            state = "Unknown"

        return state

    # Function to get Client from JOYN ID using masterAllocationSheet

    def getClient(apiNumber):
        if apiNumber in apiNumberList:
            index = apiNumberList.index(apiNumber)
            client = masterAllocationData["Asset Group"][index]
        else:
            client = "Unknown"

        return client

    #### BEGIN SCRIPT #####

    idToken = getIdToken()  # get idToken from authJoyn function

    # set correct URL for Reading Data API JOYN - use idToken as header for authorization. Note the isCustom=true is required for custom entities and todate and fromdate are based on modified timestamp and rolling 7 days lookback
    urlRolling = (
        "https://api-fdg.joyn.ai/admin/api/ReadingData?isCustom=true&entityids=15408&fromdate="
        + dateToLookBackString
        + "&todate="
        + dateTomorrowString
        + "&pagesize=1000&pagenumber="

    )

    pageNumber = 1  # set page number to 1
    nextPage = False  # set nextPage to True to start while loop
    totalResults = []  # create empty list to store results

    while not nextPage:  # loop through all pages of data
        url = urlRolling + str(pageNumber)
        # makes the request to the API
        response = requests.request(
            "GET", url, headers={"Authorization": idToken})
        # Dislay status code of response - 200 = GOOD
        if response.status_code != 200:
            print(response.status_code)

        print("Length of Response: " + str(len(response.json())))

        # get response in json format and append to totalResults list
        resultsReadingType = response.json()

        # if length of response is 0, then there is no more data to return
        if len(resultsReadingType) == 0:
            break
            # triggers while loop to end when no more data is returned and length of response is 0

        totalResults.append(resultsReadingType)

        pageNumber = pageNumber + 1  # increment page number by 1 for pagination

    # set initial variables
    readingVolume = 0
    dataSource = "di"

    # create empty dataframe to store results with correct headers for JOYN API
    headersJoynRaw = ["AssetId", "Name", "ReadingVolume",
                      "NetworkName", "Date", "Product", "Disposition"]

    headersFinal = [
        "Date",
        "Client",
        "API",
        "Well Accounting Name",
        "Oil Volume",
        "Gas Volume",
        "Water Volume",
        "Oil Sold Volume",
        "Oil Forecast",
        "Gas Forecast",
        "Water Forecast",
        "Data Source",
        "State"
    ]

    counter = 0
    # create empty dataframe to store results with correct headers for JOYN API for both raw and final data pivot
    rawJoynTotalAssetProduction = pd.DataFrame(columns=headersJoynRaw)
    currentRunTotalAssetProductionJoyn = pd.DataFrame(columns=headersFinal)

    ## Master loop through all results from JOYN API ##
    for i in range(0, len(totalResults)):
        for j in range(0, len(totalResults[i])):
            # JOYN unquie ID for each asset
            uuidRaw = totalResults[i][j]["assetId"]
            apiNumber = getApiNumber(uuidRaw)
            if apiNumber == "Unknown":
                x = 5
            wellName = getName(apiNumber)
            # reading volume for current allocation row
            readingVolume = totalResults[i][j]["Volume"]
            isDeleted = totalResults[i][j]["IsDeleted"]
            # network name for current allocation row
            networkName = totalResults[i][j]["NetworkName"]
            # reading date for current allocation row
            date = totalResults[i][j]["ReadingDate"]
            # runs splitdate() into correct format
            dateBetter = splitDateFunction(date)
            # product type for current allocation row
            productName = totalResults[i][j]["Product"]
            # runs switchProductType() to get correct product type
            newProduct = switchProductType(productName)
            # disposition for current allocation row
            disposition = totalResults[i][j]["Disposition"]

            if isDeleted == True:
                continue
            if wellName == "Read 332H":
                x = 5

            # checking to confirm that the record is not deleted or if its oil sold volume
            if disposition == 760098 or disposition == 760101 or disposition == 760094 or disposition == 760095 or disposition == 760097:
                if isDeleted != True:
                    newProduct = "Oil Sales Volume"
                else:
                    continue

            if disposition == 760096 or newProduct == "Oil Sales Volume":
                row = [apiNumber, wellName, readingVolume, networkName,
                       dateBetter, newProduct, disposition]
                # append row to dataframe
                rawJoynTotalAssetProduction.loc[len(
                    rawJoynTotalAssetProduction)] = row
            else:
                continue

    # convert date column to datetime format for sorting purposes
    rawJoynTotalAssetProduction["Date"] = pd.to_datetime(
        rawJoynTotalAssetProduction["Date"])
    forecastedProductionData["Date"] = pd.to_datetime(
        forecastedProductionData["Date"])
    # sort dataframe by date for loop to get daily production
    rawTotalAssetProductionSorted = rawJoynTotalAssetProduction.sort_values(by=[
        "Date"])
    
    rawTotalAssetProductionSorted.to_excel(workingDataDirectory + r"\production\rawTotalAssetProductionSorted.xlsx")

    # Setting some initial variables for loop
    dailyRawProduction = np.zeros([200, 4])
    dailyRawAssetId = []
    dailyRawWellName = []
    dailyRawDate = []
    dailyClientName = []
    dailyForecastedProduction = np.zeros([200, 3])
    priorDate = -999
    wellCounter = 0
    lastIndex = 0

    # Master Loop Through the raw production data to pivot from 3 records per well per date to one record per well per date with 3 columns: Oil, Gas, Water
    for i in range(0, len(rawTotalAssetProductionSorted)):
        # get current row
        row = rawTotalAssetProductionSorted.iloc[i]
        apiNumberPivot = row["AssetId"]
        readingVolumePivot = row["ReadingVolume"]
        productTypePivot = row["Product"]
        datePivot = row["Date"]
        # oilSalesPivot = row["Oil Sales Volume"]
        wellNamePivot = getName(apiNumberPivot)
        if wellNamePivot == "Unknown":
            x = 5
        stateName = getState(apiNumberPivot)
        clientName = getClient(apiNumberPivot)
        forecastVolumes = getForecast(
            apiNumberPivot, datePivot)

        # Loops through all the rows and pivots the data from 3 records per well per date to one record per well per date with 3 columns: Oil, Gas, Water and 1 column for Oil Sales Volume along with forecasted volumes
        if datePivot != priorDate and priorDate != -999:
            # prints all the data for the day
            for j in range(0, wellCounter):
                row = [dailyRawDate[j], dailyClientName[j], dailyRawAssetId[j], dailyRawWellName[j], dailyRawProduction[j][0], dailyRawProduction[j]
                       [1], dailyRawProduction[j][2], dailyRawProduction[j][3], dailyForecastedProduction[j][0], dailyForecastedProduction[j][1], dailyForecastedProduction[j][2], dataSource, stateName]

                currentRunTotalAssetProductionJoyn.loc[lastIndex +
                                                       j] = row
            # resets the daily variables
            dailyRawProduction = np.zeros([200, 4])
            dailyForecastedProduction = np.zeros([200, 3])
            dailyRawAssetId = []
            dailyRawWellName = []
            dailyRawDate = []
            dailyClientName = []
            wellCounter = 0  # keeping track of how many results we have on the same day
            lastIndex = lastIndex + j + 1
        # if API number is in the list, then we need to update the dailyRawProduction array
        if apiNumberPivot in dailyRawAssetId:
            index = dailyRawAssetId.index(apiNumberPivot)
            # update the dailyRawProduction array with correct index
            if productTypePivot == "Oil":
                dailyRawProduction[index][0] = readingVolumePivot
            elif productTypePivot == "Gas":
                dailyRawProduction[index][1] = readingVolumePivot
            elif productTypePivot == "Water":
                dailyRawProduction[index][2] = readingVolumePivot
            elif productTypePivot == "Oil Sales Volume":
                dailyRawProduction[index][3] = readingVolumePivot
        # if API number is not in the list, then we need to add it to the list and update the dailyRawProduction array
        else:
            dailyRawAssetId.append(apiNumberPivot)
            dailyRawWellName.append(wellNamePivot)
            cleanDatePivot = datePivot.strftime("%m/%d/%Y")
            dailyRawDate.append(cleanDatePivot)
            dailyClientName.append(clientName)
            dailyForecastedProduction[wellCounter][0] = forecastVolumes[0]
            dailyForecastedProduction[wellCounter][1] = forecastVolumes[1]
            dailyForecastedProduction[wellCounter][2] = forecastVolumes[2]
            if productTypePivot == "Oil":
                dailyRawProduction[wellCounter][0] = readingVolumePivot
            elif productTypePivot == "Gas":
                dailyRawProduction[wellCounter][1] = readingVolumePivot
            elif productTypePivot == "Water":
                dailyRawProduction[wellCounter][2] = readingVolumePivot
            elif productTypePivot == "Oil Sales Volume":
                dailyRawProduction[wellCounter][3] = readingVolumePivot
            wellCounter = wellCounter + 1
        # update priorDate to current date
        priorDate = datePivot

    # write the last day out after we have processed the last row of the last data
    for j in range(0, wellCounter):
        row = [dailyRawDate[j], dailyClientName[j], dailyRawAssetId[j], dailyRawWellName[j], dailyRawProduction[j][0], dailyRawProduction[j]
               [1], dailyRawProduction[j][2], dailyRawProduction[j][3], dailyForecastedProduction[j][0], dailyForecastedProduction[j][1], dailyForecastedProduction[j][2], dataSource, stateName]

        currentRunTotalAssetProductionJoyn.loc[lastIndex + j] = row

    # merge currentRunTotalAssetProductionJoyn into masterJoynData
    masterJoynData = mergeBIntoA(masterJoynData, currentRunTotalAssetProductionJoyn)

    return masterJoynData


"""
    
This script gets all current users in JOYN    

"""

def getJoynUsers(joynUsername, joynPassword, nameToFind):
    
    def getIdToken():

        load_dotenv()

        login = joynUsername
        password = joynPassword

        # User Token API
        url = "https://api.joyn.ai/common/user/token"
        # Payload for API - use JOYN crdentials
        payload = {
            "uname": str(login),
            "pwd": str(password)
        }
        # Headers for API - make sure to use content type of json
        headers = {
            "Content-Type": "application/json"
        }

        # dump payload into json format for correct format
        payloadJson = json.dumps(payload)
        response = requests.request(
            "POST", url, data=payloadJson, headers=headers)  # make request
        if response.status_code == 200:  # 200 = success
            print("Successful JOYN Authentication")
        else:
            print(response.status_code)

        results = response.json()  # get response in json format
        idToken = results["IdToken"]  # get idToken from response

        return idToken
    
    
    idToken = getIdToken()  # get idToken from authJoyn function
    
    url = "https://api-fdg.joyn.ai/admin/api/User?page=1&start=0&limit=25"
    
    response = requests.request(
            "GET", url, headers={"Authorization": idToken})
    # Dislay status code of response - 200 = GOOD
    if response.status_code != 200:
        print(response.status_code)

    print("Length of Response: " + str(len(response.json())))

    # get response in json format and append to totalResults list
    resultsReadingType = response.json()
    
    
    ## gets correct user id
    for name in resultsReadingType["Result"]: 
        nameFind = name.get("xid") # Use get method; returns None if 'xid' key is not found
        if nameFind: # This condition will be False if email is None
            if nameToFind in name["xid"]:
                id = name["id"]
                break

    return id


"""

GET Well ObjectId in order to upload production data


"""

def getWellObjectId(joynUsername, joynPassword, nameOfWell):
    
    objectId = 0
    
    def getIdToken():
            
            load_dotenv()
    
            login = joynUsername
            password = joynPassword
    
            # User Token API
            url = "https://api.joyn.ai/common/user/token"
            # Payload for API - use JOYN crdentials
            payload = {
                "uname": str(login),
                "pwd": str(password)
            }
            # Headers for API - make sure to use content type of json
            headers = {
                "Content-Type": "application/json"
            }
    
            # dump payload into json format for correct format
            payloadJson = json.dumps(payload)
            response = requests.request(
                "POST", url, data=payloadJson, headers=headers)
            
            if response.status_code == 200:  # 200 = success
                print("Successful JOYN Authentication")
            else:
                print(response.status_code)
            
            results = response.json()  # get response in json format
            idToken = results["IdToken"]
            
            return idToken
    
    idToken = getIdToken()  # get idToken from authJoyn function
    
    url = "https://api-fdg.joyn.ai/admin/api/Well?page=1&start=0&limit=1000"
    
    request = requests.request(
        "GET",
        url,
        headers={"Authorization": idToken}
    )
    
    response = request.json()
    responseCode = request.status_code
    print(responseCode)
    
    ## find specific "n" well - given nameOfWell
    
    for i in range(0, len(response["Result"])):
        wellName = response["Result"][i]["n"]
        if nameOfWell in wellName:
            objectId = response["Result"][i]["id"]
            break
    
    print(objectId)
    
    return objectId


"""
        
PUT Function - loads data from Read
      
"""

def putReadDataUpdate(userId, rawProductionData, objectId, joynUsername, joynPassword):
    
    userId = int(userId)
    
    def getIdToken():

        load_dotenv()

        login = joynUsername
        password = joynPassword

        # User Token API
        url = "https://api.joyn.ai/common/user/token"
        # Payload for API - use JOYN crdentials
        payload = {
            "uname": str(login),
            "pwd": str(password)
        }
        # Headers for API - make sure to use content type of json
        headers = {
            "Content-Type": "application/json"
        }

        # dump payload into json format for correct format
        payloadJson = json.dumps(payload)
        response = requests.request(
            "POST", url, data=payloadJson, headers=headers)  # make request
        if response.status_code == 200:  # 200 = success
            print("Successful JOYN Authentication")
        else:
            print(response.status_code)

        results = response.json()  # get response in json format
        idToken = results["IdToken"]  # get idToken from response

        return idToken
    
    file = open(r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\v2\src\kingscripts\operations\docs\Read342H_SampleUpload.json")

    dataTemplate = json.load(file)
    
    numberOfRows = len(rawProductionData)
    j = 0
    r = 1
 
    for i in range(0, numberOfRows):
        data = copy.deepcopy(dataTemplate)
        ##test zone
        day = rawProductionData["Day"][i]
        readingDate = dt.datetime.strptime(day, "%m/%d/%Y")
        readingDateClean = readingDate.strftime("%Y-%m-%d")
        id = 1000 + i ## creates unique id for each row that ties everything together
        j = j + 1
        ## Readings
        readings = copy.deepcopy(data["LCustomEntity"]["Readings"][0])
        readings["ID"] = id
        readings["ReadingID"] = id
        readings["ReadingDate"] = readingDateClean
        readings["ModifiedBy"] = userId
        readings["CreatedBy"] = userId
        readings["ObjectID"] = objectId
        readings["ReadingNumber"] = r
        readings["CreatedOn"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["ModifiedOn"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["ModifiedTimestamp"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["CreatedTimestamp"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        j = j + 1
        r = r + 1 ## updates reading number
        
        ##WRITE STATEMENT - ADD TO JSON
        if j < 3:
            data["LCustomEntity"]["Readings"][0].update(readings)
        else:
            data["LCustomEntity"]["Readings"].append(readings)
        
        ## Decs - Oil volume
        newDecs = copy.deepcopy(data["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawProductionData[" Total Oil Produced"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        if j < 4:
            data["LCustomEntity"]["custo"]["decs"][0].update(newDecs)
        else:
            data["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product Oil
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002 ## product code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760011) ## 760011 = oil
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        if j < 5:
            data["LCustomEntity"]["custo"]["ints"][0].update(newInts)
        else:
            data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## INTS - Disposition Production
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 ## dispostion code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760096) ## 760096 = poduction
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        idToken = getIdToken()  # get idToken from authJoyn function
    
        ## POST request to JOYN API
        url = "https://api-fdg.joyn.ai/mobile/api/rpc/ReadingDataUpload/bulkuploadv2"
        
        request = requests.request(
            "POST",
            url,
            json=data,
            headers={"Authorization": idToken, "Content-Type": "application/json"}
        )
    
        response = request.json()
        responseCode = request.status_code
        print(response["message"])
            
        ## STARTING MCF VOLUME
        
        data = copy.deepcopy(dataTemplate)
            
        ## Readings
        readings = copy.deepcopy(data["LCustomEntity"]["Readings"][0])
        readings["ID"] = id
        readings["ReadingID"] = id
        readings["ReadingDate"] = readingDateClean
        readings["ModifiedBy"] = userId
        readings["CreatedBy"] = userId
        readings["ObjectID"] = objectId
        readings["ReadingNumber"] = r
        readings["CreatedOn"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["ModifiedOn"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["ModifiedTimestamp"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["CreatedTimestamp"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        j = j + 1
        r = r + 1 ## updates reading number
        
        ##WRITE STATEMENT - ADD TO JSON
        if j < 3:
            data["LCustomEntity"]["Readings"][0].update(readings)
        else:
            data["LCustomEntity"]["Readings"].append(readings)
            
            
              
        ## Decs - MCF volume
        newDecs = copy.deepcopy(data["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawProductionData[" Total MCF"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product Gas
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002 ## product code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760010) ## gas
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## INTS - Disposition Production
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 ## disposition code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760096) ## 760096 = production
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        
        idToken = getIdToken()  # get idToken from authJoyn function
    
        ## POST request to JOYN API
        url = "https://api-fdg.joyn.ai/mobile/api/rpc/ReadingDataUpload/bulkuploadv2"
        
        request = requests.request(
            "POST",
            url,
            json=data,
            headers={"Authorization": idToken, "Content-Type": "application/json"}
        )
    
        response = request.json()
        responseCode = request.status_code
        print(response["message"])
        
        ## NEW READING OIL SALES VOLUME
        data = copy.deepcopy(dataTemplate)
        
        ## Readings
        readings = copy.deepcopy(data["LCustomEntity"]["Readings"][0])
        readings["ID"] = id
        readings["ReadingID"] = id
        readings["ReadingDate"] = readingDateClean
        readings["ModifiedBy"] = userId
        readings["CreatedBy"] = userId
        readings["ObjectID"] = objectId
        readings["ReadingNumber"] = r
        readings["CreatedOn"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["ModifiedOn"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["ModifiedTimestamp"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["CreatedTimestamp"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        j = j + 1
        r = r + 1 ## updates reading number
        
        ##WRITE STATEMENT - ADD TO JSON
        if j < 3:
            data["LCustomEntity"]["Readings"][0].update(readings)
        else:
            data["LCustomEntity"]["Readings"].append(readings)
        
        
        ## Decs = Oil Sold Volume
        newDecs = copy.deepcopy(data["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawProductionData[" Total Oil Sold"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002 ## product
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760011) ## oil sold
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 ## disposition
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760098) ## oil sold
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        idToken = getIdToken()  # get idToken from authJoyn function
    
        ## POST request to JOYN API
        url = "https://api-fdg.joyn.ai/mobile/api/rpc/ReadingDataUpload/bulkuploadv2"
        
        request = requests.request(
            "POST",
            url,
            json=data,
            headers={"Authorization": idToken, "Content-Type": "application/json"}
        )
    
        response = request.json()
        responseCode = request.status_code
        print(response["message"])
        
        ### STARTING WATER VOLUME
        
        data = copy.deepcopy(dataTemplate)
        
        ## Readings
        readings = copy.deepcopy(data["LCustomEntity"]["Readings"][0])
        readings["ID"] = id
        readings["ReadingID"] = id
        readings["ReadingDate"] = readingDateClean
        readings["ModifiedBy"] = userId
        readings["CreatedBy"] = userId
        readings["ObjectID"] = objectId
        readings["ReadingNumber"] = r
        readings["CreatedOn"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["ModifiedOn"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["ModifiedTimestamp"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["CreatedTimestamp"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        j = j + 1
        r = r + 1 ## updates reading number
        
        ##WRITE STATEMENT - ADD TO JSON
        if j < 3:
            data["LCustomEntity"]["Readings"][0].update(readings)
        else:
            data["LCustomEntity"]["Readings"].append(readings)
        
        ## Decs = Water Volume
        newDecs = copy.deepcopy(data["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawProductionData[" Total Water"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760012) ## water production
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## INTS - Disposition
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 # disposition
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760096) ## water production
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        idToken = getIdToken()  # get idToken from authJoyn function
    
        ## POST request to JOYN API
        url = "https://api-fdg.joyn.ai/mobile/api/rpc/ReadingDataUpload/bulkuploadv2"
        
        request = requests.request(
            "POST",
            url,
            json=data,
            headers={"Authorization": idToken, "Content-Type": "application/json"}
        )
    
        response = request.json()
        responseCode = request.status_code
        print(response["message"])
    

    return data


def putReadData(userId, rawProductionData, objectId, joynUsername, joynPassword):
    
    userId = int(userId)
    
    def getIdToken():

        load_dotenv()

        login = joynUsername
        password = joynPassword

        # User Token API
        url = "https://api.joyn.ai/common/user/token"
        # Payload for API - use JOYN crdentials
        payload = {
            "uname": str(login),
            "pwd": str(password)
        }
        # Headers for API - make sure to use content type of json
        headers = {
            "Content-Type": "application/json"
        }

        # dump payload into json format for correct format
        payloadJson = json.dumps(payload)
        response = requests.request(
            "POST", url, data=payloadJson, headers=headers)  # make request
        if response.status_code == 200:  # 200 = success
            print("Successful JOYN Authentication")
        else:
            print(response.status_code)

        results = response.json()  # get response in json format
        idToken = results["IdToken"]  # get idToken from response

        return idToken
    
    file = open(r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\v2\src\kingscripts\operations\docs\Read342H_SampleUpload.json")

    data = json.load(file)
    
    numberOfRows = len(rawProductionData)
    j = 0
    r = 1
 
    for i in range(0, numberOfRows):
        day = rawProductionData["Day"][i]
        readingDate = dt.datetime.strptime(day, "%m/%d/%Y")
        readingDateClean = readingDate.strftime("%Y-%m-%d")
        id = 1000 + i ## creates unique id for each row that ties everything together
        j = j + 1
        ## Readings
        readings = copy.deepcopy(data["LCustomEntity"]["Readings"][0])
        readings["ID"] = id
        readings["ReadingID"] = id
        readings["ReadingDate"] = readingDateClean
        readings["ModifiedBy"] = userId
        readings["CreatedBy"] = userId
        readings["ObjectID"] = objectId
        readings["ReadingNumber"] = r
        readings["CreatedOn"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["ModifiedOn"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["ModifiedTimestamp"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        readings["CreatedTimestamp"] = dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        j = j + 1
        r = r + 1 ## updates reading number
        
        ##WRITE STATEMENT - ADD TO JSON
        if j < 3:
            data["LCustomEntity"]["Readings"][0].update(readings)
        else:
            data["LCustomEntity"]["Readings"].append(readings)
        
        ## Decs - Oil volume
        newDecs = copy.deepcopy(data["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawProductionData[" Total Oil Produced"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        if j < 4:
            data["LCustomEntity"]["custo"]["decs"][0].update(newDecs)
        else:
            data["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product Oil
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002 ## product code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760011) ## 760011 = oil
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        if j < 5:
            data["LCustomEntity"]["custo"]["ints"][0].update(newInts)
        else:
            data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## INTS - Disposition Production
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 ## dispostion code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760096) ## 760096 = production
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## Decs - MCF volume
        newDecs = copy.deepcopy(data["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawProductionData[" Total MCF"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product Gas
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002 ## product code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760010) ## gas
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## INTS - Disposition Production
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 ## disposition code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760096) ## 760096 = production
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## Decs = Oil Sold Volume
        newDecs = copy.deepcopy(data["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawProductionData[" Total Oil Sold"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002 ## product
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760011) ## oil sold
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 ## disposition
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760098) ## oil sold
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## Decs = Water Volume
        newDecs = copy.deepcopy(data["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawProductionData[" Total Water"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760012) ## water production
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## INTS - Disposition
        newInts = copy.deepcopy(data["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 # disposition
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760096) ## water production
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        data["LCustomEntity"]["custo"]["ints"].append(newInts)
    
    print(type(data))
    
    dataPayload = json.dumps(data)
    print(type(dataPayload))
    
    idToken = getIdToken()  # get idToken from authJoyn function
    
    ## POST request to JOYN API
    url = "https://api-fdg.joyn.ai/mobile/api/rpc/ReadingDataUpload/bulkuploadv2"
    
    request = requests.request(
        "POST",
        url,
        json=data,
        headers={"Authorization": idToken, "Content-Type": "application/json"}
    )
    
    response = request.json()
    responseCode = request.status_code
    print(response["message"])
    
    with open(r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\v2\src\kingscripts\operations\docs\Read342H_SampleUploadModifed" + str(objectId) + ".json", "w") as outfile:
        json.dump(data, outfile)
    
    # data["LCustomEntity"]["Readings"].append(readings)
    
    return data


"""
        
For this script to work, A and B MUST have the same columns and column names.
Two of those columns must be "Date" and "API". The function will merge B into A
      
"""

def mergeBIntoA(A, B):
    # convert date column to datetime format for sorting purposes
    A["Date"] = pd.to_datetime(A["Date"])
    A["Date"] = A["Date"].dt.strftime("%m/%d/%Y")
    B["Date"] = pd.to_datetime(B["Date"])
    B["Date"] = B["Date"].dt.strftime("%m/%d/%Y")
    # compare rows in B to rows in A
    for i in range(0, len(B)):
        row = B.iloc[i]
        # get index of row in A that matches row in B
        index = A.index[(A["Date"] == row["Date"]) &
                        (A["API"] == row["API"])].tolist()
        # if no index is found, then append row to A
        if len(index) > 1:
            print("Error: More than one row found")
        # if no index is found, then append row to A
        if len(index) == 0:
            A.loc[len(A)] = row
        else:
            A.iloc[index] = row

    # convert date column to datetime format for sorting purposes
    A["Date"] = pd.to_datetime(A["Date"])
    A.sort_values(by=["Date"], inplace=True, ascending=True)
    # convert date column to string format for viewing purposes
    A["Date"] = A["Date"].dt.strftime("%m/%d/%Y")

    return A

