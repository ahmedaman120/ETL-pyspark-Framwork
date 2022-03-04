import json, os, re, sys 
import logging
from typing import Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = f"{BASE_DIR}/logs/job-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT)
logger = logging.getLogger("py4j")
sys.path.insert(1, BASE_DIR)
from classes import classPyspark  # nopep8

def main():
    
    config = openFile(f"{BASE_DIR}/json/sales.json")
    ss = startSpark(config)
    
    print(stopSpark(ss))

def openFile(dir:str) -> dict:
    def openJson(filePath):
        with open(filePath, 'r') as f:
            data = json.load(f)
            return data
    if isinstance(dir, str) and os.path.exists(dir):
        return openJson(dir)


def startSpark(config:dict) -> SparkSession:
    ss: SparkSession = classPyspark.Sparkclass(config=config).sparkStart(config)
    return ss

def stopSpark(spark:SparkSession) -> bool:
    spark.stop()
    return 
if __name__ == '__main__':
    main()
    