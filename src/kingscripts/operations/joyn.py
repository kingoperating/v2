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
def getDailyAllocatedProductionRawWithDeleted(joynUsername, joynPassword, daysToLookBack):

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

    def removeDash(string):
        string = string.replace("-", "")
        
        return string

    # Functions

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
        "UUID",
        "ReadingVolume",
        "NetworkName",
        "ReadingDate",
        "Product", 
        "Disposition", 
        "isDeleted",
        "modifiedTimestamp",
        "CreatedBy"
    ]
    

    # create empty dataframe to store results with correct headers for JOYN API for both raw and final data pivot
    rawJoynTotalAssetProduction = pd.DataFrame(columns=headersJoynRaw)

    ## Master loop through all results from JOYN API ##
    for i in range(0, len(totalResults)):
        for j in range(0, len(totalResults[i])):
            # JOYN unquie ID for each asset
            assetId = totalResults[i][j]["assetId"]
            assetId = removeDash(assetId) # remove dash for ease of SQL server
            # reading volume for current allocation row
            readingVolume = totalResults[i][j]["Volume"]
            ## ID
            # id = str(totalResults[i][j]["ID"])
            uuid = totalResults[i][j]["UUID"]
            uuid = removeDash(uuid)
            isDeleted = totalResults[i][j]["IsDeleted"]
            # network name for current allocation row
            networkName = totalResults[i][j]["NetworkName"]
            # reading date for current allocation row
            readingDate = totalResults[i][j]["ReadingDate"]
            # product type for current allocation row
            productName = totalResults[i][j]["Product"]
            # disposition for current allocation row
            disposition = totalResults[i][j]["Disposition"]
            modifedTimestamp = totalResults[i][j]["ModifiedTimestamp"]
           # comments = str(totalResults[i][j]["Comments"])
            createdBy = totalResults[i][j]["CreatedBy"]
           # objectType = totalResults[i][j]["ObjectType"]
            
            #if isDeleted == True:
              #  continue

            row = [assetId, uuid, readingVolume, networkName,
                       readingDate, productName, disposition, isDeleted, modifedTimestamp, createdBy]
                # append row to dataframe
            rawJoynTotalAssetProduction.loc[len(
                    rawJoynTotalAssetProduction)] = row
            

    # convert date column to datetime format for sorting purposes
    rawJoynTotalAssetProduction["Date"] = pd.to_datetime(
        rawJoynTotalAssetProduction["ReadingDate"])
    
    return rawJoynTotalAssetProduction


"""
    
This script gets all the modifed daily allocated production over to the master JOYN dataframe in prep to merge    

"""

def getDailyAllocatedProductionRaw(joynUsername, joynPassword, daysToLookBack):

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

    def removeDash(string):
        string = string.replace("-", "")
        
        return string

    # Functions

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
        "UUID",
        "ReadingVolume",
        "NetworkName",
        "ReadingDate",
        "Product", 
        "Disposition", 
        "isDeleted",
        "modifiedTimestamp",
        "CreatedBy"
    ]
    

    # create empty dataframe to store results with correct headers for JOYN API for both raw and final data pivot
    rawJoynTotalAssetProduction = pd.DataFrame(columns=headersJoynRaw)

    ## Master loop through all results from JOYN API ##
    for i in range(0, len(totalResults)):
        for j in range(0, len(totalResults[i])):
            # JOYN unquie ID for each asset
            assetId = totalResults[i][j]["assetId"]
            assetId = removeDash(assetId) # remove dash for ease of SQL server
            # reading volume for current allocation row
            readingVolume = totalResults[i][j]["Volume"]
            ## ID
            # id = str(totalResults[i][j]["ID"])
            uuid = totalResults[i][j]["UUID"]
            uuid = removeDash(uuid)
            isDeleted = totalResults[i][j]["IsDeleted"]
            # network name for current allocation row
            networkName = totalResults[i][j]["NetworkName"]
            # reading date for current allocation row
            readingDate = totalResults[i][j]["ReadingDate"]
            # product type for current allocation row
            productName = totalResults[i][j]["Product"]
            # disposition for current allocation row
            disposition = totalResults[i][j]["Disposition"]
            modifedTimestamp = totalResults[i][j]["ModifiedTimestamp"]
           # comments = str(totalResults[i][j]["Comments"])
            createdBy = totalResults[i][j]["CreatedBy"]
           # objectType = totalResults[i][j]["ObjectType"]
            
            if isDeleted == True:
                continue

            row = [assetId, uuid, readingVolume, networkName,
                       readingDate, productName, disposition, isDeleted, modifedTimestamp, createdBy]
                # append row to dataframe
            rawJoynTotalAssetProduction.loc[len(
                    rawJoynTotalAssetProduction)] = row
            

    # convert date column to datetime format for sorting purposes
    rawJoynTotalAssetProduction["Date"] = pd.to_datetime(
        rawJoynTotalAssetProduction["ReadingDate"])
    
    return rawJoynTotalAssetProduction


"""
    
This script gets all current users in JOYN    

"""

def getJoynUser(joynUsername, joynPassword, nameToFind):
    
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

GET Well header data 

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
    
    results = response["Result"]
    
    ## flatten nested json
    data = pd.json_normalize(results)
    data["UUID"] = data["UUID"].str.replace("-", "")
    
    return data


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

def putJoynData(userId, rawData, objectId, joynUsername, joynPassword):
    
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
    
    numberOfRows = len(rawData)
    j = 0
    r = 1
    
    print("Starting Looping...")
    ## Master loop, goes through all five days and updates JOYN
    for i in range(0, numberOfRows):
        sampleData = copy.deepcopy(dataTemplate)
        ##test zone
        day = rawData["Date"][i]
        readingDate = dt.datetime.strptime(day, "%m/%d/%Y")
        readingDateClean = readingDate.strftime("%Y-%m-%d")
        id = 1000 + i ## creates unique id for each row that ties everything together
        j = j + 1
        ## Readings
        readings = copy.deepcopy(sampleData["LCustomEntity"]["Readings"][0])
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
            sampleData["LCustomEntity"]["Readings"][0].update(readings)
        else:
            sampleData["LCustomEntity"]["Readings"].append(readings)
        
        ## Decs - Oil volume
        newDecs = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawData["Oil"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        if j < 4:
            sampleData["LCustomEntity"]["custo"]["decs"][0].update(newDecs)
        else:
            sampleData["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product Oil
        newInts = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002 ## product code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760011) ## 760011 = oil
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        if j < 5:
            sampleData["LCustomEntity"]["custo"]["ints"][0].update(newInts)
        else:
            sampleData["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## INTS - Disposition Production
        newInts = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 ## dispostion code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760096) ## 760096 = poduction
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        sampleData["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        idToken = getIdToken()  # get idToken from authJoyn function
    
        ## POST request to JOYN API
        joynApiPost(sampleData, idToken)
        
        ## STARTING MCF VOLUME
        
        sampleData = copy.deepcopy(dataTemplate)
            
        ## Readings
        readings = copy.deepcopy(sampleData["LCustomEntity"]["Readings"][0])
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
            sampleData["LCustomEntity"]["Readings"][0].update(readings)
        else:
            sampleData["LCustomEntity"]["Readings"].append(readings)
            
            
              
        ## Decs - MCF volume
        newDecs = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawData["Gas"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        sampleData["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product Gas
        newInts = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002 ## product code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760010) ## gas
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        sampleData["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## INTS - Disposition Production
        newInts = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 ## disposition code
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760096) ## 760096 = production
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        sampleData["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        
        idToken = getIdToken()  # get idToken from authJoyn function
        ## POST request to JOYN API
        joynApiPost(sampleData, idToken)
    
        ## NEW READING OIL SALES VOLUME
        sampleData = copy.deepcopy(dataTemplate)
        
        ## Readings
        readings = copy.deepcopy(sampleData["LCustomEntity"]["Readings"][0])
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
            sampleData["LCustomEntity"]["Readings"][0].update(readings)
        else:
            sampleData["LCustomEntity"]["Readings"].append(readings)
        
        
        ## Decs = Oil Sold Volume
        newDecs = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawData["Oil Sold"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        sampleData["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product
        newInts = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002 ## product
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760011) ## oil sold
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        sampleData["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        newInts = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 ## disposition
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760098) ## oil sold
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        sampleData["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        idToken = getIdToken()  # get idToken from authJoyn function
    
        ## POST request to JOYN API
        joynApiPost(sampleData, idToken)
        
        ### STARTING WATER VOLUME
        
        sampleData = copy.deepcopy(dataTemplate)
        
        ## Readings
        readings = copy.deepcopy(sampleData["LCustomEntity"]["Readings"][0])
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
            sampleData["LCustomEntity"]["Readings"][0].update(readings)
        else:
            sampleData["LCustomEntity"]["Readings"].append(readings)
        
        ## Decs = Water Volume
        newDecs = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["decs"][0])
        newDecs["attId"] = 263005
        newDecs["v"] = str(rawData["Water"][i])
        newDecs["ID"] = j
        newDecs["ReadingID"] = id
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        sampleData["LCustomEntity"]["custo"]["decs"].append(newDecs)
        
        ## INTS - Product
        newInts = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263002
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760012) ## water production
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        sampleData["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        ## INTS - Disposition
        newInts = copy.deepcopy(sampleData["LCustomEntity"]["custo"]["ints"][0])
        newInts["attId"] = 263003 # disposition
        newInts["ID"] = j
        newInts["ReadingID"] = id
        newInts["v"] = str(760096) ## water production
        j = j + 1
        
        ## WRITE STATEMENT - ADD TO JSON
        sampleData["LCustomEntity"]["custo"]["ints"].append(newInts)
        
        idToken = getIdToken()  # get idToken from authJoyn function
    
        ## POST request to JOYN API
        joynApiPost(sampleData, idToken)
    
    print("Finished Looping " + str(objectId))

    return sampleData


"""
        
Compares JOYN and SQL data to find duplicates and returns a list of duplicate UUIDs
      
"""

def compareJoynSqlDuplicates(sqlData, joynData):
    
    sqlIds = sqlData["UUID"].tolist()
    joynIds = joynData["UUID"].tolist()
    
    ## compare two lists and return duplicates
    duplicateRecordId = list(set(sqlIds) & set(joynIds))
    
    print("Number of Duplicate Records: " + str(len(duplicateRecordId)))
    print("All Historcal Records Length Prior to Delete: " + str(len(sqlIds)))
    print("Number of JOYN Records: " + str(len(joynIds)))
    
    ## conver to dataframe with header "ID"
    duplicateRecordId = pd.DataFrame(duplicateRecordId, columns=["UUID"])
    duplicateRecordId["UUID"] = duplicateRecordId["UUID"].astype(str)
    
    return duplicateRecordId


"""
        
Gets the product types from JOYN API
      
"""

def getProductList(joynUsername, joynPassword):
    
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


"""
        
Gets all the picklistoptions and returns the JSON response
      
"""

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


"""
        
Gets all the Entity ID's and returns the JSON response allows for different dataviews
      
"""

def getEntityIdList():
        
        load_dotenv()
        
        joynUsername = os.getenv("JOYN_USERNAME")
        joynPassword = os.getenv("JOYN_PASSWORD")
        
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
        
        url = "https://api-fdg.joyn.ai/admin/api/ReadingView"
        
        request = requests.request(
            "GET",
            url,
            headers={"Authorization": idToken}
        )
        
        response = request.json()
        responseCode = request.status_code
        print(responseCode)
        x=5
        
        headers = ["id", "n"]
        entityIdTable = pd.DataFrame(columns=headers)
        
        for i in range(0, len(response["Result"])):
            id = response["Result"][i]["id"]
            name = response["Result"][i]["n"]
            row = [id, name]
            entityIdTable.loc[len(entityIdTable)] = row
    
        return entityIdTable


"""
        
Gets the comments for all wells in daily well reading
      
"""

def getDailyWellReading(joynUsername, joynPassword, daysToLookBack):

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

    def removeDash(string):
        string = string.replace("-", "")
        
        return string

    # Functions

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

    #### BEGIN SCRIPT #####

    idToken = getIdToken()  # get idToken from authJoyn function

    # set correct URL for Reading Data API JOYN - use idToken as header for authorization. Note the isCustom=true is required for custom entities and todate and fromdate are based on modified timestamp and rolling 7 days lookback
    urlRolling = (
        "https://api-fdg.joyn.ai/admin/api/ReadingData?isCustom=true&entityids=15404&fromdate="
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

    # Unpack Total Results into dataframe
    headers = [
        "UUID",
        "assetId",
        "IsDeleted",
        "Comments",
        "ReadingDate",
        "ModifiedTimestamp",
        "CasingPressure",
        "SurfaceCasingPressure",
        "TubingPressure",
        "ObjectID",
        "CreatedBy",
        "Downtime",
        "DowntimeReason",
        "FlowlinePressure",
        "FluidLevel",
        "InjectionPressure"
    ]

    masterComments = pd.DataFrame(columns=headers)
    for i in range(0, len(totalResults)):
        for j in range(0, len(totalResults[i])):
            # JOYN unquie ID for each asset
            uuid = totalResults[i][j]["UUID"]
            uuid = removeDash(uuid)
            # AssetID for current allocation row
            assetId = totalResults[i][j]["assetId"]
            # isDeleted for current allocation row
            isDeleted = totalResults[i][j]["IsDeleted"]
            # Comments for current allocation row
            comments = str(totalResults[i][j]["Comments"])
            # reading date for current allocation row
            readingDate = totalResults[i][j]["ReadingDate"]
            # modified timestamp for current allocation row
            modifiedTimestamp = totalResults[i][j]["ModifiedTimestamp"]
            # casing pressure for current allocation row
            casingPressure = totalResults[i][j]["CasingPressure"]
            # surface casing pressure for current allocation row
            surfaceCasingPressure = totalResults[i][j]["SurfaceCasingPressure"]
            # tubing pressure for current allocation row
            tubingPressure = totalResults[i][j]["TubingPressure"]
            # object id for current allocation row
            objectId = totalResults[i][j]["ObjectID"]
            # created by for current allocation row
            createdBy = totalResults[i][j]["CreatedBy"]
            # downtime for current allocation row
            downtime = totalResults[i][j]["Downtime"]
            # downtime reason for current allocation row
            downtimeReason = totalResults[i][j]["DowntimeReason"]
            # flowline pressure for current allocation row
            flowlinePressure = totalResults[i][j]["FlowLinePressure"]
            # fluid level for current allocation row
            fluidLevel = totalResults[i][j]["FluidLevel"]
            # Injection Pressure for current allocation row
            InjectionPressure = totalResults[i][j]["InjectionPressure"]

            row = [
                uuid,
                assetId,
                isDeleted,
                comments,
                readingDate,
                modifiedTimestamp,
                casingPressure,
                surfaceCasingPressure,
                tubingPressure,
                objectId,
                createdBy,
                downtime,
                downtimeReason,
                flowlinePressure,
                fluidLevel,
                InjectionPressure
            ]

            # append row to dataframe
            masterComments.loc[len(masterComments)] = row

    return masterComments
