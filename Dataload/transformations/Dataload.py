
import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
     

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadCSV").getOrCreate()
     

df_sales=spark.read.format('csv').option('header',True).option('inferSchema','True').load('/Volumes/workspace/default/my/sales_data.csv')
     

df_mapping=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/Volumes/workspace/default/my/customer_salesperson_mapping.csv')
     

df_sales.show()
df_mapping.printSchema()
df_mapping.display()