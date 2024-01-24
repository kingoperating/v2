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

def getData(server, database, tableName):
    # Set up the connection parameters
    server = server
    database = database
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
    
    print("Data has been pulled from the " + str(tableName) + " in the " + str(database) + " database on the " + str(server) + " server.")

    # Return the dataframe
    return data


"""
    
PUT Function - replaces entire table with dataframe given serverName, databaseName, and tableName from king-arc1
    
"""

def putDataReplace(server, database, data, tableName):
    
    # Set up the connection parameters
    # Establish the connection with Windows Authentication
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(connection_string))
    
    data.to_sql(tableName, engine, if_exists='replace', index=False, chunksize = 25000)
    
    print("Data has been replaced to the " + str(tableName) + " in the " + str(database) + " database on the " + str(server) + " server.")
   

def putDataAppend(server, database, data, tableName):
    
    # Set up the connection parameters
    # Establish the connection with Windows Authentication
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(connection_string))
    
    data.to_sql(tableName, engine, if_exists='append', index=False, chunksize = 25000)
    
    print("Data has been appended to the " + str(tableName) + " in the " + str(database) + " database on the " + str(server) + " server.")
   
   
    
def putDataUpdate(server, database, data, tableName):
    
    # Set up the connection parameters
    # Establish the connection with Windows Authentication
    # Establish the connection with Windows Authentication
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    connection = pyodbc.connect(connection_string)
    # Create a cursor object to interact with the database
    cursor = connection.cursor()
    
    # for index, row in data.iterrows():
    #     columns = ', '.join([f'{col}' for col in row.index])  # Enclose column names in square brackets
    #     placeholders = ', '.join(['?'] * len(row))
    #     sql = f'INSERT INTO {tableName} {columns} VALUES {placeholders}'
    #     cursor.execute(sql, row)
    
    for i, row in data.iterrows():
        columns = ', '.join(row[i].index)
        values = ', '.join(['?'] * len(row))
        sql = f'INSERT INTO {tableName} {columns} VALUES {values}'
        cursor.execute(sql, tuple(row))

    # Commit the transaction
    connection.commit()

    # Close the connection
    connection.close()
    
    print("Data has been added to the " + str(tableName) + " in the " + str(database) + " database on the " + str(server) + " server.")
    
    test = 5


## This function will take a list of duplicate records and delete them from the database
    
def deleteDuplicateRecords(server, database, tableName, duplicateList):
        
        # Set up the connection parameters
        # Establish the connection with Windows Authentication
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    connection = pyodbc.connect(connection_string)
        # Create a cursor object to interact with the database
    cursor = connection.cursor()

    # loop through the duplicate list and delete the rows with the duplicate IDs
    
    try:
        for recordId in duplicateList:
            recordId = str(recordId)
            sql = f"DELETE FROM {tableName} WHERE UUID = ?"
            cursor.execute(sql, recordId)
            # Commit the transaction
            connection.commit()
    
    except pyodbc.Error as e:
        print("Database error:", e)
        
    cursor.close()
    connection.close()

# The below code was an attempt to delete the duplicate records in batches to improve runtime. While it did work, it was not faster than the above code.
# We will keep it here for reference in case we need to use it in the future - will revisit after looking into multi-threading. 

# def deleteDuplicateRecords(server, database, tableName, duplicateList):
#     # Set up the connection parameters
#     connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    
#     # Establish the connection with Windows Authentication
#     connection = pyodbc.connect(connection_string)
    
#     # Create a cursor object to interact with the database
#     cursor = connection.cursor()

#     try:
#         # Begin the transaction
#         connection.autocommit = False

#         # Loop through the duplicate list and delete the rows with the duplicate IDs in batches
#         batch_size = 1000  # Adjust the batch size based on your requirements
#         for i in range(0, len(duplicateList), batch_size):
#             batch_ids = duplicateList[i:i + batch_size]
#             # Use parameterized query with explicit parameter type
#             sql = f"DELETE FROM {tableName} WHERE UUID = CAST(? AS NVARCHAR(MAX))"
#             cursor.executemany(sql, [(str(recordId),) for recordId in batch_ids])

#         # Commit the transaction
#         connection.commit()

#     except pyodbc.Error as e:
#         # Rollback the transaction in case of an error
#         connection.rollback()
#         print("Database error:", e)
    
#     finally:
#         # Ensure to close cursor and connection
#         cursor.close()
#         connection.close()