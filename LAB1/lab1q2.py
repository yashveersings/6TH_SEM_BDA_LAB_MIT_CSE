import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import os
import sys
import pandas as pd

os.environ['PYSPARK_PYTHON']=sys.executable
os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable

spark=SparkSession.builder.config("spark.driver.memory","16g").appName('max').getOrCreate()
df_pd=pd.DataFrame(data={'Integer':[1,2,],'Float':[1.0,2.0,3.0]})
df=spark.createDataFrame(df_pd)
max=df.select('Integer').rdd.max()[0]
print(x)
