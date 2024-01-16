import requests
import json
import datetime as dt
import re
import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np
from pathlib import Path
import copy
from kingscripts.operations.combocurve import *
from kingscripts.operations import docs

"""
    
This script gets all the modifed daily allocated production over to the master JOYN dataframe in prep to merge    

"""
def getDailyAllocatedProductionRawWithDeleted(joynUsername, joynPassword, wellHeaderData, daysToLookBack):

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

   

    # Functions
    
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

    # Function to get API number from wellHeaderData "xid" using uuid from JOYN API
    def getApiNumber(uuid):
        if uuid in wellHeaderData["UUID"].tolist():
            index = wellHeaderData["UUID"].tolist().index(uuid)
            apiNumberYay = wellHeaderData["xid"][index]
        else:
            apiNumberYay = "Unknown"

        return apiNumberYay
    
    ## Function to get well name "n" from wellHeaderData using "uuid" from JOYN API
    def getName(uuid):
        if uuid in wellHeaderData["UUID"].tolist():
            index = wellHeaderData["UUID"].tolist().index(uuid)
            name = wellHeaderData["n"][index]
        else:
            name = "Unknown"

        return name
    

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

    # create empty dataframe to store results with correct headers for JOYN API
    headersJoynRaw = [
        
        "AssetId", 
        "ID",
        "Name",
        "ReadingVolume",
        "NetworkName",
        "ReadingDate",
        "Product", 
        "Disposition", 
        "isDeleted",
        "modifiedTimestamp",
        "Comments",
        "CreatedBy",
        "ObjectType"
    ]
    

    # create empty dataframe to store results with correct headers for JOYN API for both raw and final data pivot
    rawJoynTotalAssetProduction = pd.DataFrame(columns=headersJoynRaw)

    ## Master loop through all results from JOYN API ##
    for i in range(0, len(totalResults)):
        for j in range(0, len(totalResults[i])):
            # JOYN unquie ID for each asset
            uuidRaw = totalResults[i][j]["assetId"]
            apiNumber = getApiNumber(uuidRaw)
            # reading volume for current allocation row
            readingVolume = totalResults[i][j]["Volume"]
            ## ID
            id = str(totalResults[i][j]["ID"])
            isDeleted = totalResults[i][j]["IsDeleted"]
            # network name for current allocation row
            networkName = totalResults[i][j]["NetworkName"]
            niceName = getName(uuidRaw)
            # reading date for current allocation row
            readingDate = totalResults[i][j]["ReadingDate"]
            # runs splitdate() into correct format
            dateBetter = splitDateFunction(readingDate)
            # product type for current allocation row
            productName = totalResults[i][j]["Product"]
            # disposition for current allocation row
            disposition = totalResults[i][j]["Disposition"]
            modifedTimestamp = totalResults[i][j]["ModifiedTimestamp"]
            comments = str(totalResults[i][j]["Comments"])
            createdBy = totalResults[i][j]["CreatedBy"]
            objectType = totalResults[i][j]["ObjectType"]

            row = [apiNumber, id, niceName, readingVolume, networkName,
                       dateBetter, productName, disposition, isDeleted, modifedTimestamp, comments, createdBy, objectType]
                # append row to dataframe
            rawJoynTotalAssetProduction.loc[len(
                    rawJoynTotalAssetProduction)] = row
            

    # convert date column to datetime format for sorting purposes
    rawJoynTotalAssetProduction["Date"] = pd.to_datetime(
        rawJoynTotalAssetProduction["ReadingDate"])
    # sort dataframe by date for loop to get daily production
    rawTotalAssetProductionSorted = rawJoynTotalAssetProduction.sort_values(by=[
        "Date"])
    
    return rawTotalAssetProductionSorted


"""
    
This script gets all the modifed daily allocated production over to the master JOYN dataframe in prep to merge    

"""
def getDailyAllocatedProductionRaw(joynUsername, joynPassword, wellHeaderData, daysToLookBack):

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

   

    # Functions
    
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

    # Function to get API number from wellHeaderData "xid" using uuid from JOYN API
    def getApiNumber(uuid):
        if uuid in wellHeaderData["UUID"].tolist():
            index = wellHeaderData["UUID"].tolist().index(uuid)
            apiNumberYay = wellHeaderData["xid"][index]
        else:
            apiNumberYay = "Unknown"

        return apiNumberYay
    
    ## Function to get well name "n" from wellHeaderData using "uuid" from JOYN API
    def getName(uuid):
        if uuid in wellHeaderData["UUID"].tolist():
            index = wellHeaderData["UUID"].tolist().index(uuid)
            name = wellHeaderData["n"][index]
        else:
            name = "Unknown"

        return name
    

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

    # create empty dataframe to store results with correct headers for JOYN API
    headersJoynRaw = [
        
        "AssetId", 
        "ID",
        "Name",
        "ReadingVolume",
        "NetworkName",
        "ReadingDate",
        "Product", 
        "Disposition", 
        "isDeleted",
        "modifiedTimestamp",
        "Comments",
        "CreatedBy",
        "ObjectType"
    ]
    

    # create empty dataframe to store results with correct headers for JOYN API for both raw and final data pivot
    rawJoynTotalAssetProduction = pd.DataFrame(columns=headersJoynRaw)

    ## Master loop through all results from JOYN API ##
    for i in range(0, len(totalResults)):
        for j in range(0, len(totalResults[i])):
            # JOYN unquie ID for each asset
            uuidRaw = totalResults[i][j]["assetId"]
            apiNumber = getApiNumber(uuidRaw)
            # reading volume for current allocation row
            readingVolume = totalResults[i][j]["Volume"]
            ## ID
            id = str(totalResults[i][j]["ID"])
            isDeleted = totalResults[i][j]["IsDeleted"]
            # network name for current allocation row
            networkName = totalResults[i][j]["NetworkName"]
            niceName = getName(uuidRaw)
            # reading date for current allocation row
            readingDate = totalResults[i][j]["ReadingDate"]
            # runs splitdate() into correct format
            dateBetter = splitDateFunction(readingDate)
            # product type for current allocation row
            productName = int(totalResults[i][j]["Product"])
            # disposition for current allocation row
            disposition = int(totalResults[i][j]["Disposition"])
            modifedTimestamp = totalResults[i][j]["ModifiedTimestamp"]
            comments = str(totalResults[i][j]["Comments"])
            createdBy = totalResults[i][j]["CreatedBy"]
            objectType = totalResults[i][j]["ObjectType"]

            if isDeleted == True:
                continue
        
            row = [apiNumber, id, niceName, readingVolume, networkName,
                       dateBetter, productName, disposition, isDeleted, modifedTimestamp, comments, createdBy, objectType]
                # append row to dataframe
            rawJoynTotalAssetProduction.loc[len(
                    rawJoynTotalAssetProduction)] = row
            

    # convert date column to datetime format for sorting purposes
    rawJoynTotalAssetProduction["Date"] = pd.to_datetime(
        rawJoynTotalAssetProduction["ReadingDate"])
    # sort dataframe by date for loop to get daily production
    rawTotalAssetProductionSorted = rawJoynTotalAssetProduction.sort_values(by=[
        "Date"])
    
    return rawTotalAssetProductionSorted

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

GET Well API and ObjectID master table

"""

def getWellHeaderData(joynUsername, joynPassword):
    

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
    
    ## dump response into dataframe
    wellHeaderData = pd.DataFrame(response["Result"])
    
    return wellHeaderData


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

def putJoynDataApi(userId, rawProductionData, objectId, joynUsername, joynPassword):
    
    userId = int(userId)
    
    def joynApiPost(dataJson, idToken):
        ## POST request to JOYN API
        url = "https://api-fdg.joyn.ai/mobile/api/rpc/ReadingDataUpload/bulkuploadv2"
        
        request = requests.request(
            "POST",
            url,
            json=dataJson,
            headers={"Authorization": idToken, "Content-Type": "application/json"}
        )
    
        response = request.json()
        responseCode = request.status_code
        if responseCode != 200:
            print(responseCode)
            print(response["message"])
        else:
            m = 5
    
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
            m = 5
        else:
            print(response.status_code)

        results = response.json()  # get response in json format
        idToken = results["IdToken"]  # get idToken from response

        return idToken
    
    # Gets the Sample Upload Template from docs
    pathBetter  = Path(__file__).parent / r"docs\Read342H_SampleUpload.json"
    file = open(pathBetter, "r")
    dataTemplate = json.load(file)
    
    numberOfRows = len(rawProductionData)
    j = 0
    r = 1
    
    print("Starting Looping...")
    ## Master loop, goes through all five days and updates JOYN
    for i in range(0, numberOfRows):
        data = copy.deepcopy(dataTemplate)
        ##test zone
        day = rawProductionData["Date"][i]
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
        newDecs["v"] = str(rawProductionData["Oil"][i])
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
        joynApiPost(data, idToken)
        
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
        newDecs["v"] = str(rawProductionData["Gas"][i])
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
        joynApiPost(data, idToken)
    
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
        newDecs["v"] = str(rawProductionData["Oil Sold"][i])
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
        joynApiPost(data, idToken)
        
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
        newDecs["v"] = str(rawProductionData["Water"][i])
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
        joynApiPost(data, idToken)
    
    print("Finished Looping " + str(objectId))

    return data


def compareJoynSqlDuplicates(sqlData, joynData):
    
    sqlIds = sqlData["ID"].tolist()
    joynIds = joynData["ID"].tolist()
    
    ## compare two lists and return duplicates
    duplicateRecordId = list(set(sqlIds) & set(joynIds))
    
    print("Number of Duplicate Records: " + str(len(duplicateRecordId)))
    print("All Historcal Records Length Prior to Delete: " + str(len(sqlIds)))
    print("Number of JOYN Records: " + str(len(joynIds)))
    
    ## conver to dataframe with header "ID"
    duplicateRecordId = pd.DataFrame(duplicateRecordId, columns=["ID"])
    duplicateRecordId["ID"] = duplicateRecordId["ID"].astype(str)
    
    return duplicateRecordId


def getProductType(joynUsername, joynPassword):
    
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
    
    url = "https://api-fdg.joyn.ai/admin/api/ProductType"
    
    request = requests.request(
        "GET",
        url,
        headers={"Authorization": idToken}
    )
    
    response = request.json()
    responseCode = request.status_code
    print(responseCode)
    
    headers = ["id", "n"]
    productTypeTable = pd.DataFrame(columns=headers)
    
    for i in range(0, len(response["Result"])):
        id = response["Result"][i]["id"]
        name = response["Result"][i]["n"]
        row = [id, name]
        productTypeTable.loc[len(productTypeTable)] = row

    return productTypeTable


def getPicklistOptions(joynUsername, joynPassword):
    
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
    
    url = "https://api-fdg.joyn.ai/admin/api/PicklistOptions"
    
    request = requests.request(
        "GET",
        url,
        headers={"Authorization": idToken}
    )
    
    response = request.json()
    responseCode = request.status_code
    print(responseCode)
    
    x= 5
    
    return x