import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
import sys
os.environ["PYSPARK_PYTHON"]=sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"]=sys.executable
spark=SparkSession.builder.config("pyspark.driver.memory","16g").appName("readCSV").getOrCreate()
df=spark.read.csv('data.csv',header=True,inferSchema=True)
df.show()
