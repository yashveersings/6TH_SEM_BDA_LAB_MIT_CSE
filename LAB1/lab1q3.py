import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import pandas as pd
import numpy as np
import os
import sys
os.environ["PYSPARK_PYTHON"]=sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"]=sys.executable
spark=SparkSession.builder.config("spark.driver.memory","16g").appName('average').getOrCreate()
df_pd=pd.DataFrame(data={'Integer':[1,2,3],'Float':[1.0,2.0,3.0]})
df=spark.createDataFrame(df_pd)
x=df.agg(f.avg(df['Integer']))
x.show()

    
    