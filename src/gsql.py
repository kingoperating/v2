## Gabe's Test Environment for pulling SQL Data from Server ##

import os
from kingscripts.analytics import tech
import pandas as pd

kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATABASE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))



data = tech.getData(
    serverName=kingLiveServer,
    databaseName=kingProductionDatabase,
    tableName="prod_bad_pumper_data"
)

data.head()