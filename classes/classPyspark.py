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

    def sparkStart(self,kwargs:dict) ->SparkSession:
        appName: str = kwargs['spark_conf']['appname'] 
        MASTER: str  = kwargs['spark_conf']['master']
        LOGLEVEL: str  = kwargs['log']['level1']
        LOGLEVEL2: str  = kwargs['log']['level2']


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
        
    