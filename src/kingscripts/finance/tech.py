import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Returns all the IT Spend Coded by Michael Tanner in using coding tool - returns a dataframe


def getItSpend(serverName, databaseName):
    # Set up the connection parameters
    server = str(os.getenv('SQL_SERVER'))
    database = str(os.getenv('SQL_KING_DATABASE'))
    # Establish the connection with Windows Authentication
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    connection = pyodbc.connect(connection_string)
    # Create a cursor object to interact with the database
    cursor = connection.cursor()
    # Execute a SQL query to fetch data from the "itSpend" table
    query = 'SELECT * FROM itSpend'
    cursor.execute(query)
    # Fetch all the rows from the query result
    rows = cursor.fetchall()
    # Get the column names from the cursor object
    columnNames = [column[0] for column in cursor.description]
    # Create a Pandas DataFrame from the fetched data
    itSpendData = pd.DataFrame.from_records(rows, columns=columnNames)
    # Convert the "DateCoded" column to datetime format
    itSpendData["DateCoded"] = pd.to_datetime(
        itSpendData["DateCoded"], format="%Y-%m-%d")

    # Close the cursor and the connection
    cursor.close()
    connection.close()

    # Return the dataframe
    return itSpendData
