## Gabe's Test Environment for pulling SQL Data from Server ##

# Performing  GIT PUSH

# git checkout -b testgsql

import os
from kingscripts.analytics import tech
from kingscripts.operations import joyn
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# load .env file
load_dotenv()

kingLiveServer = str(os.getenv('SQL_SERVER_KING_DATABASE'))
kingProductionDatabase = str(os.getenv('SQL_PRODUCTION_DATABASE'))


# How to query

# data = tech.getData(
#     serverName=kingLiveServer,
#     databaseName=kingProductionDatabase,
#     tableName="prod_bad_pumper_data"
# )

data = np.random.rand(10, 10)
data = pd.DataFrame(data)

# How to write 

# king_gabe_table = str(os.getenv('SQL_GABE_DATABASE'))
# tech.putData(
#     server=kingLiveServer,
#     database = king_gabe_table,
#     data = data,
#     tableName= "test_table"
# )
# print("Hooray")

joynUsername = str(os.getenv('JOYN_USERNAME'))
joynPassword = str(os.getenv('JOYN_PASSWORD'))

wellHeaderData = joyn.getWellHeaderData(
    joynUsername=joynUsername,
    joynPassword=joynPassword
)

dataframe = joyn.getDailyAllocatedProductionRawWithDeleted(joynUsername, joynPassword, wellHeaderData, 5)

dataframe.to_csv(r"C:\Users\gtatman\Downloads\Python\testproduction.csv")
