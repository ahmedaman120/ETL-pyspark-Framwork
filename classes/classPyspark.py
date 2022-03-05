# nopep8
import json, os, re, sys
from typing import Callable, Optional


from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

# from pyspark import SparkContext


class Sparkclass:
    spark:SparkSession
    def __init__(self, config:dict):  # , config
        self.config = config

    def importData(self,ss:SparkSession, folderPath:str, pattern:str)->DataFrame:
        #add check if the path file or directory
        def fileOrDir(folderPath:str)->str:
            if isinstance(folderPath ,str) and os.path.exists(folderPath):
                if os.path.isdir(folderPath):
                    return "dir"
                elif os.path.isfile(folderPath):
                    return "file"
        
        #function to open dir
        def openDir(getUFilesExtension:Callable,dir: str, pattern:Optional[str]=None)->list:
            filesList = []
            if isinstance(dir ,str) and os.path.exists(dir):
                    filesList = self._listDir(dir,pattern)
                    exts = getUFilesExtension(filesList)
                    return filesList,exts
        def openFile(getUFilesExtension:Callable,dir:str):
            filesListF = []
            if isinstance(dir ,str) and os.path.exists(dir):
                    filesListF.append(dir)
                    exts = getUFilesExtension(filesListF)
                    return filesListF,exts
        def getFilesExtension(files:list) -> list:
            if isinstance(files, list) and len(files) > 0:
                exts = list(set([os.path.splitext(f)[1] for f in files]))
                return exts[0] if len(exts) ==1 else None
        pathType = fileOrDir(folderPath)
        # print(pathType)
        if pathType =='dir':
           files,exts= openDir(getFilesExtension,folderPath,pattern)
        elif pathType=='file':
           files,exts= openFile(getFilesExtension,folderPath)
        return self.createDataFrame(ss,files,exts)
    
    def createDataFrame(self, spark:SparkSession,files:list,filetype:str)-> DataFrame:
        # print()
        def creatFromCSV(fileList:list)->DataFrame:
            print("***********csv***********")
            fileList2 =["file://" +f for f in fileList]
            df = spark.read.format("csv")\
                            .option("header","true")\
                            .option("mode","DROPMALFORMED")\
                            .load(fileList2)
            
            return df
            
        def creatFromJSON(fileList:list)->DataFrame:
            print("***********json***********")
            fileList2 =["file://" +f for f in fileList]
            df = spark.read.format("json") \
                            .option("mode","PERMISSIVE") \
                            .option("permissiveAsString","true") \
                            .load(fileList2)
            return df
            
        print(filetype)
        if filetype =='.json':
           return creatFromJSON(files)
        elif filetype == '.csv':
            return creatFromCSV(files)
        

    
    def _listDir(self, dir:str,pattern:Optional[str]=None) ->list:
        def recursiveFileList(dir:str):
            if os.path.exists(dir):
                filesList = []
                for dirpath, dirname, filename in os.walk(dir):
                    for file in filename:
                        filesList.append(f"{dirpath}/{file}")
                return filesList

        def filterPatter(dir:str, pattern:Optional[str]=None):
            # print("i am performed")
            return [x for x in recursiveFileList(dir) if re.search(rf'{pattern}',x)]
        return recursiveFileList(dir) if pattern ==None else filterPatter(dir,pattern)
    
    

    def sparkStart(self,kwargs:dict) ->SparkSession:
        appName: str = kwargs['spark_conf']['appname'] 
        MASTER: str  = kwargs['spark_conf']['master']
        LOGLEVEL: str  = kwargs['log']['level1']
        # LOGLEVEL2: str  = kwargs['log']['level2']


        def startSparkSession(master:Optional[str]= "local[*]"
                                , app:Optional[str]="app",
                                log:Optional[str]='ALL') ->SparkSession:
            spark = SparkSession.builder\
                .appName(app)\
                .master(master)\
                .getOrCreate()
            # spark.sparkContext.setLogLevel(log)
            # spark.sparkContext.setLogLevel(LOGLEVEL2)
            return spark
        
        def getSession(spark:SparkSession) -> None:
            print(f"\033[1;33m{spark}\033[0m")
            print(f"\033[96m{spark.sparkContext.getConf().getAll()}\033[0m")

        self.spark = startSparkSession(MASTER,appName)
        getSession(self.spark)

        return startSparkSession(self.spark)