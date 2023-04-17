# King Operating Corporation Python Package

This package allows for easy access to the majority of KOC data products. Currently on version 2.0.0.

Developed and Maintained by Michael Tanner. Please email mtanner@kingoperating.com with any questions.

Visit [KOC Development Site](https://mtanner161.github.io/kingdashboard/#/kingdashboard) for our ongoing front-end application development

## Documentation

Use `git clone` download and access packages. There are 3 different modules withing `kingscripts` - `afe`, `operations` and `analytics`. Each of these packages connect with different data products within the King ecosystem.

To import these modules, first `git clone https://github.com/kingoperating/v2.git`

Then, import the packages below:

```python
from kingscripts.operations import greasebook, combocurve
from kingscripts.random import enverus
from kingscripts.afe import afe
```

## afe Module

One (1) package `afe.py` and two (2) functions

1.  `afe.dailyCost` - calculates and outputs two csv files, daysvsdepth.csv and dailyItemCost.csv for given `nameOfWell`

    - Note: see `afe.py` to set correct paths to data folder
    - Arguments
      - `name`: Name of the well, see masterWellList for details `str`

2.  `afe.variance`

    - Note: see `afe.py` to set correct paths to data folder
      - Arguments
        - `name`: Name of the well, see masterWellList for details

## production Module

Two (2) packages `greasebook` and `combocurve` and four (4) functions

1. `greasebook.getProductionData` - pulls production data from Greasebook, formats and exports clean CSV

   - Arguments
     - `pullProd`: `True` to pull all GB production data, `False` to pull limited number and update master file
     - `days`: Number of days to pull - if `pullProd=True` set to 0 `int`
     - `greasebookApi`: Greasebook API key `str`

2. `combocurve.putWellProductionData` - requests data from Greasebooks and inserts into ComboCurve

   - Arguments
     - `pullFromAllocation`: `True` to pull all GB production data, `False` to pull limited number and update master file
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`
     - `daysToPull`: Number of days to pull, if `pullFromAllocation=True` set to 0 `int`

3. `combocurve.getLastestScenario` - pull the latest scenerio given a projectId and scenerioId
   - Arguments
     - `projectIdKey`: ComboCurve specific project id - get through front-end UI `str`
     - `scenarioIdKey`: ComboCurve specific scenerio id - get through front-end UI `str`
     - `serviceAccount`: ComboCurve Service Account - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `object`
     - `comboCurveApi`: ComboCurve Api connection - see [ComboCurve PyPI](https://pypi.org/project/combocurve-api-v1/) `json`

## analytics Module

One package `enverus` and two (2) functions

1. `enverus.getWellData` - gets latest daily production for given well

   - Arguments:
     - `apiKey`: Enverus API authentication `object`
     - `wellApi`: Well API14 of interest `str`

2. `enverus.checkWellStatus` - chekcs the status a specific operator in a specific basin
   - Arguments:
     - `apiKey`: Enverus API authentication `object`
     - `operatorName`: Name of operator of interest `str`
     - `basin`: Name of basin of interest `str
