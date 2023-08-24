import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()

# Returns all the IT Spend Coded by Michael Tanner in using coding tool - returns a dataframe


"""
    
GET Function - returns a dataframe given serverName, databaseName, and tableName from king-arc1
    
"""

def getData(serverName, databaseName, tableName):
    # Set up the connection parameters
    server = serverName
    database = databaseName
    # Establish the connection with Windows Authentication
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    connection = pyodbc.connect(connection_string)
    # Create a cursor object to interact with the database
    cursor = connection.cursor()
    # Execute a SQL query to fetch data from the "itSpend" table
    query = 'SELECT * FROM ' + tableName
    cursor.execute(query)
    # Fetch all the rows from the query result
    rows = cursor.fetchall()
    # Get the column names from the cursor object
    columnNames = [column[0] for column in cursor.description]
    # Create a Pandas DataFrame from the fetched data
    data = pd.DataFrame.from_records(rows, columns=columnNames)


    # Close the cursor and the connection
    cursor.close()
    connection.close()

    # Return the dataframe
    return data


"""
    
PUT Function - replaces entire table with dataframe given serverName, databaseName, and tableName from king-arc1
    
"""

def putData(server, database, data, tableName):
    
    # Set up the connection parameters
    # Establish the connection with Windows Authentication
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(connection_string))
    
    data.to_sql(tableName, engine, if_exists='replace', index=False)
    
    print("Data has been added to the " + str(tableName) + " in the " + str(database) + " database on the " + str(server) + " server.")
    
     