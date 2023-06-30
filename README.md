# kingscripts Python Package

This open source python package allows for easy access to the majority of KOC data products for KOC employees. Currently on version 3.0.0

Developed and Maintained by Michael Tanner and Gabe Tatman. Please email mtanner@kingoperating.com with any questions.

Visit [KOC Development Site](https://mtanner161.github.io/kingdashboard/#/kingdashboard) for our ongoing front-end application development

## Documentation

There are four different modules withing `kingscripts` - `afe`, `finance` `operations` and `analytics`. Each of these packages connect with different data products within the King ecosystem.

To import these modules, you must `git clone https://github.com/kingoperating/v2.git` and then connect to our KOC Datawarehouse. See Michael Tanner for access to database

Then, import the packages below:

```python
from kingscripts.operations import greasebook, combocurve, joyn
from kingscripts.analytics import enverus
from kingscripts.afe import afe
from kingscripts.finance import tech, wenergy
```

## operations Module

Three (3) packages `greasebook`, `joyn` and `combocurve`

### greasebook

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

### combocurve

5. `combocurve.putJoynWellProductionData` - takes last modifed JOYN data and loads it into ComboCurve

   - Arguments
     - `currentJoynData`: pandas Dataframe of modifed data in JOYN for last 7 days `dataframe`
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`

6. `combocurve.putGreasebookWellProductionData` - requests data from Greasebooks and inserts into ComboCurve

   - Arguments
     - `workingDataDirectory`: Data directory where all exports and imports come from `str`
     - `pullFromAllocation`: `True` to pull all GB production data, `False` to pull limited number and update master file
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`
     - `daysToPull`: Number of days to pull, if `pullFromAllocation=True` set to 0 `int`

7. `combocurve.getLatestScenario` - returns pandas dataframe of the latest scenerio given a projectId and scenerioId

   - Arguments
     - `workingDataDirectory`: Data directory where all exports and imports come from `str`
     - `projectIdKey`: ComboCurve specific project id - get through front-end UI `str`
     - `scenarioIdKey`: ComboCurve specific scenerio id - get through front-end UI `str`
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`

8. `combocurve.putGreasebookWellComments` - takes Greasebook well comments and load them in ComboCurve under customString2

   - Arguments
     - `cleanJson`: takes JSON formatted comment dataset `json`
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`

9. `combocurve.getDailyForecastVolume` - gets daily forecast volumes for given CC project ID and forecast ID

   - Arguments
     - `projectIdKey`: ComboCurve specific project id - get through front-end UI `str`
     - `forecastIdKey`: ComboCurve specific scenerio id - get through front-end UI `str`
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`

### joyn

10. `joyn.getDailyAllocatedProduction` - returns pandas dataframe of modified production data from JOYN during given data length

11. `joyn.mergeBIntoA` - merges two dataframes together and updates A if B is different

    - Arguments
      - `A`: pandas dataframe which will be updated `dataframe`
      - `B`: pandas datafrme which will do the updating `dataframe`

## analytics Module

One package `enverus` and functions 2 functions

12. `enverus.getWellData` - returns pandas dataframe of monthly oil/gas/water production

- Arguments:
  - `apiKey`: Enverus API authentication `object`
  - `wellApi14`: Well API14 of interest `str`

13. `enverus.checkWellStatus` - chekcs the status a specific operator in a specific basin

    - Arguments:
      - `apiKey`: Enverus API authentication `object`
      - `operatorName`: Name of operator of interest `str`
      - `basin`: Name of basin of interest `str

## finance Module

Two packages `tech` and `wenergy`

### tech

14. `tech.getItSpend` - returns a dataframe all of all coded invoices by M. Tanner

- Arguments:
  - None

## afe Module

One (1) package `afe.py` and three (3) functions

15. `afe.dailyCost` - calculates and outputs two csv files, daysvsdepth.csv and dailyItemCost.csv for given `nameOfWell`


    - Note: see `afe.py` to set correct paths to data folder
    - Arguments
      - `workingDataDirectory`: Data directory where all exports and imports come from `str`
      - `name`: Name of the well, see masterWellList for details `str`

16. `afe.variance`


    - Note: see `afe.py` to set correct paths to data folder
      - Arguments
        - `workingDataDirectory`: Data directory where all exports and imports come from `str`
        - `name`: Name of the well, see masterWellList for details

17. `afe.combineAfeFiles`


    - Note: see `afe.py` to set correct paths to data folder
      - Arguments
        - `workingDataDirectory`: Data directory where all exports and imports come from `str`
        - `listOfWells`: List of wells in KOC Datawarehouse afe folder and is a `list`

### wenergy

`wenergy` - COMING SOON
