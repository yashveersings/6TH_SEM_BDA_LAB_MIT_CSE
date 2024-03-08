import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
import sys
import pandas as pd

os.environ["PYSPARK_PYTHON"]=sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"]=sys.executable
spark=SparkSession.builder.config("spark_driver_memory","16g").appName("Filter").getOrCreate()

data={'age':[15,20,23],'name':['abc','xyz','123']}
df_pd=pd.DataFrame(data=data)
df=spark.createDataFrame(df_pd)
filtered=df.filter(df['age']>20)
transformed=df.withColumn("age after 10 years",df['age']+10)
filtered.show()
transformed.show()
