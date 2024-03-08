import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os
import sys
import pandas as pd

os.environ["PYSPARK_PYTHON"]=sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"]=sys.executable

spark=SparkSession.builder.config("spark_driver_memory","16g").appName("count").getOrCreate()

data={'name':['abc','xyz','123'],'age':[12,20,23],'salary':[10,20,30]}
df=pd.DataFrame(data)

df=spark.createDataFrame(df)
path='lab2q4.csv'
df.write.csv(path,header=True,mode="overwrite")