from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd

sc = SparkContext('local')
spark = SparkSession(sc)

d=(spark.read.csv('./PracticaBigData/datos/cards.csv',header=True,inferSchema=True,sep = '|',)).toPandas()
d = d.withColumn('IMPORTE', regexp_replace('IMPORTE', ',', '.'))
#print(d.dtypes)
#i=d["IMPORTE"].astype(float, errors="raise")
#i=d["IMPORTE"].replace(',','.')
#g=i.groupby(by = "SECTOR")["IMPORTE"].mean()
print(i)
#print(g)