import logging
import os
import json
import argparse
import pandas as pd
from pandas.core.frame import DataFrame
import sqlalchemy
import psycopg2



DB_NAME='cogi.db'
#Define our connection string
with open("local.settings.json", "r") as jsonConfigFile:
    config = json.load(jsonConfigFile)
    print("Read successful")
    
conn_string = config["CONNECTION_STRING"]
engine_string=config["ENGINE_STRING"]

# print the connection string we will use to connect
# print ("Connecting to database\n", conn_string)

# get a connection, if a connect cannot be made an exception will be raised here
# conn = psycopg2.connect(conn_string)

def get_database_connection():
    # con = sqlite3.connect(DB_NAME)
    con=psycopg2.connect(conn_string)
    return con

def get_arguments():
    parser=argparse.ArgumentParser(description='Upload Excel files to SQL DB')
    parser.add_argument('file',help='path of the file to upload into database')
    parser.add_argument('tableName',help='The name of the table to upload data to')
    # parser.add_argument('schema',default=None,help='The schema of thable, if table already exist use this for validation only')
    parser.add_argument('-d','--date',help='the date of this file, if specified a column called date will be added to DB')
    return parser.parse_args()

def table_exist(DBconnection,tableName):
    cursor=DBconnection.cursor()
    sql="SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name=%s"
    data=(tableName,)
    cursor.execute(sql,data)
    return cursor.fetchone()!=None

def get_table_columns(DBconnection,tableName):
    cursor=DBconnection.cursor()
    cursor.execute("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =%s",(tableName,))
    columns=[]
    for column in cursor.fetchall():
        columns.append(column[0])
    print(columns)
    return columns
def align_dataframe(dataframe,DBcolumns):
    DFcolumns=dataframe.columns.values.tolist()
    print("df columns are")
    print(DFcolumns)
    DBOnlyColumns=list(set(DBcolumns)-set(DFcolumns))
    print('columns in DB only')
    print(DBOnlyColumns)
    for column in DBOnlyColumns:
        dataframe[column]=None
    DFcolumns=dataframe.columns.values.tolist()
    print("df new columns are")
    print(DFcolumns)
    DBOnlyColumns=list(set(DBcolumns)-set(DFcolumns))
    print('new columns in DB only')
    print(DBOnlyColumns)
    #drop extra columns from DF
    DFOnlyColumns=list(set(DFcolumns)-set(DBcolumns))
    for column in DFOnlyColumns:
        dataframe.drop(column,axis='columns', inplace=True)
    


def run():
    args=get_arguments()
    #1.Get file and date from arguments
    path=args.file
    tableName=args.tableName
    # schema=args.schema
    date=None
    if args.date is not None:
        date=args.date
    print('input file ',path, 'and date, ',date)
    #2.Get DataFrame from File
    data=pd.read_excel(path)
    #3.add date column if specified
    if date is not None:
        data['date']=date
    #4.connect to DB( use engine)
    engine = sqlalchemy.create_engine(engine_string)
    conn=get_database_connection()
    # create_table(cursor,data,tableName)
    #5.Create table if not already exist, and populate it with data
    print(table_exist(conn,tableName))
    if table_exist(conn,tableName):
        DBcolumns=get_table_columns(conn,tableName)
        align_dataframe(data,DBcolumns)

    data.to_sql(tableName,engine,index=False,if_exists='append')
    #7.close DB connection 
    conn.close()

run()





# def create_table():
#     """
#     Creates a table ready to accept our data.

#     write code that will execute the given sql statement
#     on the database
#     """

#     create_table = """ CREATE TABLE authors(
#         ID          INTEGER PRIMARY KEY     AUTOINCREMENT,
#         author      TEXT                NOT NULL,
#         title       TEXT                NOT NULL,
#         pages       INTEGER             NOT NULL,
#         due_date    CHAR(15)            NOT NULL
#     )   
#     """
#     con = get_database_connection()
#     con.execute(create_table)
#     con.close()

