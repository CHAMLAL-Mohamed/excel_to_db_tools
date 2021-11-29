#The goal of this script is to monitor the folder that needs to be migrated to DB
#and upload new files to DB

#When the tool is just launched it should read the last creation date save
#Folder|LastCreationDate| 
#Each Line should be |Folder|Date|TableName|patterns|
import pandas as pd
import logging
import os
import glob
import datetime
import subprocess
# The Observer watches for any file change and then dispatches the respective events to an event handler.
# from watchdog.observers import Observer
# The event handler will be notified when an event occurs.
# from watchdog.events import FileSystemEventHandler
import time


def main():
    #TODO 1: read configuration file (excel file)
    config=pd.read_excel("configuration.xlsx") #.to_dict(orient='list')
    print(config)
    print(config.dtypes)
    for index,row in config.iterrows():
        #TODO 2: for each row read all files in folder with creation date bigger than row['CreationDate']
        filesPatterns=[row['Folder']+'/*'+'.xlsx',]
        files=[]
        for pattern in filesPatterns:
            files.extend(glob.glob(pattern))
        new_files=[f for f in list(files) if datetime.datetime.strptime(time.ctime(os.path.getctime(f)),"%a %b %d %H:%M:%S %Y") > row['CreationDate']]
        new_files.sort(key=os.path.getctime)
        #TODO 3:for each newFile upload it to DB based on creteria defined in config, and save creationDate to configuration (if max)
        for file in new_files:
            #Upload the file to DB
            path=file
            tableName=row['TableName']
            date=row['Date']
            if date=='None':
                date=None
            if date is None:
                subprocess.run(["python","excel-to-db.py",path,tableName])
            else:
                subprocess.run(["python","excel-to-db.py",path,tableName,"-d",date])
            #Update creationDate in config, and save it to configuration file
            config.at[index,'CreationDate']=datetime.datetime.strptime(time.ctime(os.path.getctime(file)),"%a %b %d %H:%M:%S %Y")
            print(config.at[index,'CreationDate'])
    config.to_excel("configuration.xlsx",index=False)
    #TODO 4: create observer, and schedule for each folder













if __name__ == '__main__':
    main()