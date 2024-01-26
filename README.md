# kingscripts Python Package

<p align="center">
  <img src=".\src\images\logo.png" width="400" title="kingscripts">
</p>

This open source python package allows for easy access to the majority of KOC data products for KOC employees. Currently on version 3.4.0

Developed and Maintained by Michael Tanner and Gabe Tatman. Please email development@kingoperating.com with any questions.

Visit [KOC Development Site](https://mtanner161.github.io/kingdashboard/#/kingdashboard) for our ongoing front-end application development!

## Documentation

There are four different modules withing `kingscripts` - `afe`, `finance` `operations` and `analytics`. Each of these packages connect with different data products within the King ecosystem.

To import these modules, you must `git clone https://github.com/kingoperating/v2.git` and then connect to our KOC Datawarehouse. See Michael Tanner for access to database

Then, import the packages below:

```python
from kingscripts.operations import greasebook, combocurve, joyn
from kingscripts.analytics import enverus, king, tech
from kingscripts.afe import afe
from kingscripts.finance import wenergy
```

## operations Module

Three (3) packages `greasebook`, `joyn` and `combocurve`

### greasebook (Legacy)

1. `greasebook.getProductionData` - pulls production data from Greasebook, formats and exports CSV into working data folder

   - Arguments
     - `workingDataDirectory`: Data directory where all exports and imports come from `str`
     - `pullProd`: `True` to pull all GB production data, `False` to pull limited number and update master file
     - `days`: Number of days to pull - if `pullProd=True` set to 0 `int`
     - `greasebookApi`: Greasebook API key `str`

2. `greasebook.getComments` - returns a JSON object of all comments within greasebook within 150 days up to 250

   - Arguments -`workingDataDirectory`: Data directory where all exports and imports come from `str`
     - `greasebookApi`: Greasebook API key `str`

3. `greasebook.allocateWells` - returns a dataframe containing allocated well volumes

   - Arguments
     - `pullProd`: `True` to pull all GB production data, `False` to pull limited number and update master file
     - `days`: Number of days to pull - if `pullProd=True` set to 0 `int`
     - `workingDataDirectory`: Data directory where all exports and imports come from `str`
     - `greasebookApi`: Greasebook API key `str`
     - `edgeCaseRollingAverage`: for all edge case wells, set to `int` value to replace oil volume with rolling average

4. `greasebook.sendPumperEmail` - sends email to specific users for pumpers who have missed there data

   - Arugments:
     - `pumperNotReportedList`: list of `str` that represent pumpers who have failed to submit production data
     - `workingDataDirectory`: Data directory where all exports and imports come from `str`

5. `greasebook.getTankGauges` - returns a dataframe of tank gauge data for a given well

   - Arguments
     - `greasebookApi`: Greasebook API key `str`
     - `startDate`: Start date of tank gauge data `datetime`
     - `endDate`: End date of tank gauge data `datetime`

### combocurve

6. `combocurve.putJoynWellProductionData` - takes last modifed JOYN data and loads it into ComboCurve

   - Arguments
     - `currentJoynData`: pandas Dataframe of modifed data in JOYN for last 7 days `dataframe`
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`

7. `combocurve.putGreasebookWellProductionData` - requests data from Greasebooks and inserts into ComboCurve

   - Arguments
     - `workingDataDirectory`: Data directory where all exports and imports come from `str`
     - `pullFromAllocation`: `True` to pull all GB production data, `False` to pull limited number and update master file
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`
     - `daysToPull`: Number of days to pull, if `pullFromAllocation=True` set to 0 `int`

8. `combocurve.getLatestScenario` - returns pandas dataframe of the latest scenerio given a projectId and scenerioId

   - Arguments
     - `workingDataDirectory`: Data directory where all exports and imports come from `str`
     - `projectIdKey`: ComboCurve specific project id - get through front-end UI `str`
     - `scenarioIdKey`: ComboCurve specific scenerio id - get through front-end UI `str`
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`

9. `combocurve.putGreasebookWellComments` - takes Greasebook well comments and load them in ComboCurve under customString2

   - Arguments
     - `cleanJson`: takes JSON formatted comment dataset `json`
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`

10. `combocurve.getDailyForecastVolume` - gets daily forecast volumes for given CC project ID and forecast ID

   - Arguments
     - `projectIdKey`: ComboCurve specific project id - get through front-end UI `str`
     - `forecastIdKey`: ComboCurve specific scenerio id - get through front-end UI `str`
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`

11. `combocurve.getLatestScenarioMonthly` - gets monthly aggerate cash flow statement for given CC project ID and scenarioI ID

- Arguments
  - `projectIdKey`: ComboCurve specific project id - get through front-end UI `str`
  - `forecastIdKey`: ComboCurve specific scenerio id - get through front-end UI `str`
  - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
  - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`

12. `combocurve.ccScenarioToCrestFpSingleWell` - injests getLatestScenarioMonthly() pandas dataframe and converts it to crestFp Single Well Forecast
    
    - Arguments
      - `comboCurveScenarioData`: pandas dataframe with specific rows and columns that have discounted net present values - `dataframe`
      - `nglYield`: Natural Gas Liquids yield - 1 represents 100% yield
      - `gasBtuFactor`: gas BTU factor - always 1 unless gas is paid off BTU factor `double`
      - `gasShrinkFactor`: gas shrinkage factor (0-1) `double`
      - `oilPricePercent`: 1 unless oil price differential is not 100% `double`
      - `gasPricePercent`: 1 unless gas price differential is not 100% `double`
      - `nglPricePercnet`: 1 unless ngl price differential is not 100% `double`
      - `oilVariableCost`: $/bbl LOE cost `double`
      - `gasVariableCost`: $/MCF LOE cost `double`
      - `nglVariableCost`: $/bbl LOE cost `double`
      - `waterVariableCost`: water dispoal cost ($/bbl) `double`
      - `state`: state intials - only `tx` avaiable at the moment `str`

13. `combocurve.ccScenarioToCrestFpPdp` - injests getLatestScenarioMonthly() pandas dataframe and converts it to crestFp PDP Forecast
- Arguments
      - `comboCurveScenarioData`: pandas dataframe with specific rows and columns that have discounted net present values - `dataframe`
      - `nglYield`: Natural Gas Liquids yield - 1 represents 100% yield
      - `gasBtuFactor`: gas BTU factor - always 1 unless gas is paid off BTU factor `double`
      - `gasShrinkFactor`: gas shrinkage factor (0-1) `double`
      - `oilPricePercent`: 1 unless oil price differential is not 100% `double`
      - `gasPricePercent`: 1 unless gas price differential is not 100% `double`
      - `nglPricePercnet`: 1 unless ngl price differential is not 100% `double`


### joyn

12. `joyn.getDailyAllocatedProductionRawWithDeleted` - returns pandas dataframe of modified production data from JOYN during given data length with deleted wells

    - Arguments
      - `joynUsername` - username for login to JOYN `str`
      - `joynPassword` - password for JOYN `str`
      - `daysToLookBack` - how many days to look back for modified production data? `int`

13. `joyn.getDailyAllocatedProductionRaw` - returns pandas dataframe of modified production data from JOYN during given data length with and without deleted wells

    - Arguments
      - `joynUsername` - username for login to JOYN `str`
      - `joynPassword` - password for JOYN `str`
      - `daysToLookBack` - how many days to look back for modified production data? `int`

14. `joyn.getJoynUser` - returns specific user XID from JOYN give a name

    - Arguments
      - `joynUsername` - username for login to JOYN `str`
      - `joynPassword` - password for JOYN `str`
      - `nameToFind` - name of JOYN user to find `str` 
 
15. `joyn.getWellObjectId` - returns specific well object id from JOYN given a well name

    - Arguments
      - `joynUsername` - username for login to JOYN `str`
      - `joynPassword` - password for JOYN `str`
      - `nameOfWell` - name of JOYN well to find `str`

16. `joyn.putJoynData` - loads data from dataframe into JOYN - see `operations.docs` for sample upload`
      - Arguments
        - `userId` - JOYN user id which is return in `getJoynUser` - `str` 
        - `data` - pandas dataframe of data to load into JOYN `dataframe`
        - `joynUsername` - username for login to JOYN `str`
        - `joynPassword` - password for JOYN `str`
        
17. `joyn.compareJoynSqlDuplicates` - compares JOYN data to SQL data to find duplicates

    - Arguments
      - `sqlData` - data from `daily_production` table in SQL `dataframe`
      - `joynData` - last XX days of modified JOYN data `dataframe`

18. `joyn.getProductList` - returns product listing for a given well

    - Arguments
      - `joynUsername` - username for login to JOYN `str`
      - `joynPassword` - password for JOYN `str`

19. `joyn.getPicklistOptions` - returns picklist options and returns JSON

    - Arguments
      - `joynUsername` - username for login to JOYN `str`
      - `joynPassword` - password for JOYN `str`

20. `joyn.getEntityIdList` - returns entity id list for a given well

    - Arguments
      - `joynUsername` - username for login to JOYN `str`
      - `joynPassword` - password for JOYN `str`

21. `joyn.getDailyWellReading` - returns a dataframe with the well comments from the Daily Well Reading View
  
      - Arguments
        - `joynUsername` - username for login to JOYN `str`
        - `joynPassword` - password for JOYN `str`
        - `daysToLookBack` - how many days to look back for modified production data `int`


## analytics Module

Two (2) packages `enverus` and `king` with 3 functions

### enverus

22. `enverus.getWellData` - returns pandas dataframe of monthly oil/gas/water production

- Arguments:
  - `apiKey`: Enverus API authentication `object`
  - `wellApi14`: Well API14 of interest `str`

23. `enverus.checkWellStatus` - chekcs the status a specific operator in a specific basin

    - Arguments:
      - `apiKey`: Enverus API authentication `object`
      - `operatorName`: Name of operator of interest `str`
      - `basin`: Name of basin of interest `str

### king

24. `king.sendEmail`

    - Arugments:
      - `emailRecipient` - email address of the person to email `str`
      - `emailRecipientName` - name of the person emailing - used for looping `str`
      - `emailSubject` - email subject line `str`
      - `emailMessage` - email message body `str`
      - `nameOfFile` (optional) name of the file - user created
      - `attachment` - (optional) attachment to be sent (Excel File only)

25. `king.getAverageDailyVolumes`

    - Arugments:
      - `masterKingProdData` - master allocated volumes excel sheet in KOC Datawarehouse `dataframe`
      - `startDate` - YYYY-MM-DD `datetime`
      - `endDate` - YYYY-MM-DD `datetime`

26. `king.getNotReportedPumperList` - refactored pumper naughty list which creates and returns a list of pumpers who have not entered there data

    - Arugments:
    - `masterKingProdData` - master allocated volumes excel sheet in KOC Datawarehouse `dataframe`
    - `checkDate` - date to check `datetime`

27. `king.createPumperMessage` - takes a list of names and orders them by well to create the email message that goes out the operations team about missing pumper data on a given data

    - Arugments
      - `badPumperData` - dataframe of wells,names and pumper number for naughty list `dataframe`
      - `badPumperTrimmedList` - list of bad pumpers trimmed to only unquie pumper names `list`
      - `badPumperMessage` - any header information you want to begin the message with `str`

28. `king.getHCEFProduction` - gets raw HCEF production data from KOC Datawarehouse

    - Arguments
      - `pathToFolder` - path to Read data `str`

29. `king.getWorlandUnit108Production` - gets raw HCEF production data from KOC Datawarehouse

    - Arguments
      - `pathToFolder` - path to Worland data `str`

30. `king.updateUsageStatsEtl` - returns dataframe of select usage stats for ETL
    
    - Arguments
      - `etlStartTime` - start time of ETL `datetime`

31. `king.updateUsageStatsEtlRuntime` - returns dataframe of select usage stats for ETL runtime and automatically loads into SQL stats 
    
    - Arguments
      - `etlStartTime` - start time of ETL `datetime`
      - `etlEndTime` - end time of ETL `datetime`
      - `function` - name of function `str`
      - `runtime`- runtime of function `str`

## finance Module

Two packages `tech` and `wenergy`

### tech

32. `tech.getData` - returns a dataframe given KOC Datawarehouse parameters

    - Arguments:
      - `serverName` - name of server `str`
      - `databaseName` - name of database `str`
      - `tableName` - specific table to access `str`

33. `tech.putDataReplace` - replaces entire table with dataframne given KOC Datawarehouse parameters

    - Arguments:
      - `serverName` - name of server `str`
      - `databaseName` - name of database `str`
      - `tableName` - specific table to access - optional `str`

34. `tech.putDataAppend` - appends entire table with dataframne given KOC Datawarehouse parameters

    - Arguments:
      - `serverName` - name of server `str`
      - `databaseName` - name of database `str`
      - `tableName` - specific table to access - optional `str`

35. `tech.deleteDuplicateRecords` - deletes the duplicateIdList from SQL server

    - Arguments:
       - `serverName` - name of server `str`
       - `databaseName` - name of database `str`
       - `tableName` - specific table to access - optional `str`
       - `duplicateList` - list of duplicate ids to delete `list`

## afe Module

One (1) package `afe.py` and three (3) functions

36. `afe.dailyCost` - calculates and outputs two csv files, daysvsdepth.csv and dailyItemCost.csv for given `nameOfWell`

    - Note: see `afe.py` to set correct paths to data folder
    - Arguments
      - `workingDataDirectory`: Data directory where all exports and imports come from `str`
      - `name`: Name of the well, see masterWellList for details `str`

37. `afe.variance`

    - Note: see `afe.py` to set correct paths to data folder
      - Arguments
        - `workingDataDirectory`: Data directory where all exports and imports come from `str`
        - `name`: Name of the well, see masterWellList for details

38. `afe.combineAfeFiles`

    - Note: see `afe.py` to set correct paths to data folder
      - Arguments
        - `workingDataDirectory`: Data directory where all exports and imports come from `str`
        - `listOfWells`: List of wells in KOC Datawarehouse afe folder and is a `list`

### wenergy

`wenergy` - COMING SOON
