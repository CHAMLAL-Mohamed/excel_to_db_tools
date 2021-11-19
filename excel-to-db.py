import logging
import os
import json
import sys
import re
import argparse
import pandas as pd
from pandas.core.frame import DataFrame
import sqlalchemy
import psycopg2



DB_NAME='cogi.db'
#Define our connection string
with open("local.settings.json", "r") as jsonConfigFile:
    config = json.load(jsonConfigFile)
    
conn_string = config["CONNECTION_STRING"]
engine_string=config["ENGINE_STRING"]


def get_database_connection():
    # con = sqlite3.connect(DB_NAME)
    con=psycopg2.connect(conn_string)
    return con

def date(date):
    datePattern=r'^(19|20)\d\d(-?)(0[1-9]|1[012])\2(0[1-9]|[12][0-9]|3[01])$'
    if date is None:
        print('specified None as date')

    elif date=='fileName':
        print('specified fileName as date')
    elif date=='createdDate':
        print('specified createdDate as date')
    elif re.match(datePattern,date):
        print('specified ',date,' as date')  
    else :
        print('no valide date format was provided')
        raise ValueError
    return date

def file(path):
    if not os.path.isfile(path):
        raise ValueError
    return path

def get_arguments():
    parser=argparse.ArgumentParser(description='Upload Excel files to SQL DB')
    parser.add_argument('file',type=file,help='path of the file to upload into database')
    parser.add_argument('tableName',help='The name of the table to upload data to')
    # parser.add_argument('schema',default=None,help='The schema of thable, if table already exist use this for validation only')
    parser.add_argument('-d','--date',type=date,help='the date of this file, if specified a column called date will be added to DB')
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
    print(DFcolumns)
    DBOnlyColumns=list(set(DBcolumns)-set(DFcolumns))

    for column in DBOnlyColumns:
        dataframe[column]=None
    DFcolumns=dataframe.columns.values.tolist()
    DBOnlyColumns=list(set(DBcolumns)-set(DFcolumns))
    #drop extra columns from DF
    DFOnlyColumns=list(set(DFcolumns)-set(DBcolumns))
    for column in DFOnlyColumns:
        dataframe.drop(column,axis='columns', inplace=True)
    

def get_date_value(date,filePath):
    datePattern=r'(19|20)\d\d(-?)(0[1-9]|1[012])\2(0[1-9]|[12][0-9]|3[01])'
    if re.match(datePattern,date):
        return date
    elif date=='fileName':
        fileNameWithExt=os.path.basename(filePath)
        fileName=os.path.splitext(fileNameWithExt)[0]
        print(fileName)
        date=re.search(datePattern,fileName).group()
        print(date)
        return date
    elif date=='createdDate':
        date=os.path.getctime(filePath)
        return date
    else:
        raise ValueError
    
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
        data['date']=get_date_value(date,path)
    #4.connect to DB( use engine)
    engine = sqlalchemy.create_engine(engine_string)
    conn=get_database_connection()
    # create_table(cursor,data,tableName)
    #5.Create table if not already exist, and populate it with data
    print(table_exist(conn,tableName))
    if table_exist(conn,tableName):
        DBcolumns=get_table_columns(conn,tableName)
        align_dataframe(data,DBcolumns)
#TODO: 
    data.to_sql(tableName,engine,index=False,if_exists='append')
    #7.close DB connection 
    conn.close()

run()
