#!/usr/bin/env python3
#The Aim from this script is to migrate entire folders, to a database, using the tool excel_to_db as subprocess
#1.GET arguments from command line
#the folder name will be used as the tableName
#date can be extracted either from the fileName, or from the attribute "Date Created"
#if table was not already created the first file in folder will be the template
#Note: there is no garanty of files sequence, so no garanties which file will be used as template for the table

import logging
import argparse
import os
import sys
import re
import subprocess

EXCEL_EXTENSION=".xlsx"


#1.Specify the arguments: FolderPath/filePath(no subfolders will be traversed for now),
#1.1 How to specify date column if needed:fileName, Created Date, or constant
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
def directory(path):
    if not os.path.isdir(path):
        raise ValueError
    return path

def get_arguments():
    parser=argparse.ArgumentParser(description='Upload Excel files to SQL DB')
    parser.add_argument('path',type=directory,help='path of the folder to migrate to Database, the table name will be the folderName, so make sure to use comprehensive names')
    # parser.add_argument('schema',default=None,help='The schema of thable, if table already exist use this for validation only')
    parser.add_argument('-d','--date',type=date,help='''the date of this file, if specified a column called date will be added to DB\r\n
    This arguments accepts values:\r\n
    Constant date: provided in a valid format yyyymmdd\r\n
    None: no date will be used or added to DB\r\n
    fileName: the date will be parsed from fileName, and if no date found an error will be raised\r\n
    createdDate: the date will be parsed from file metadata''')
    return parser.parse_args()

def main(*args, **kwargs):
    args= get_arguments()
    try:
        for entry in os.scandir(args.path):
            if  entry.is_file() and entry.name.lower().endswith(EXCEL_EXTENSION):
                print(entry.name)
                fullPath=os.path.abspath(os.path.join(args.path,entry.name))
                tableName=os.path.basename(args.path).lower()
                if args.date is None:
                    subprocess.run(["python","excel-to-db.py",fullPath,tableName])
                else:
                    subprocess.run(["python","excel-to-db.py",fullPath,tableName,"-d",args.date])
    except NotADirectoryError:
        print("The provided path is not a directory, please check your arguments, Path: ",args.path)



if __name__ == '__main__':
    main(*sys.argv)
    
    
