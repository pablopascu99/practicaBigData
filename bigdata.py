
from pandas.core.indexing import convert_from_missing_indexer_tuple
from pyspark.sql.functions import sequence, to_date, explode, col
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import sys

sc = SparkContext('local')
spark = SparkSession(sc)

df2 = spark.read.csv("./datos/codigoPostalCoordenadas.csv",header=True,inferSchema=True,sep = ';')
df3 = spark.read.csv("./datos/weather.csv",header=True,inferSchema=True,sep = ';')
df1 = spark.read.csv("./datos/cards.csv",header=True,inferSchema=True,sep = '|')

#Numero de operaciones que se destina en alimentacion en los tiempos de maxima humedad 

m1=df1[(df1["SECTOR"]=="ALIMENTACION")]
m=m1.groupBy("DIA").sum("NUM_OP") 
k=df3.join(m,df3.FECHA == m.DIA, "left").select(m["DIA"] ,df3["HumMax"],m["sum(NUM_OP)"])
k.groupBy("DIA").sum("sum(NUM_OP)")
k.show(30) 

#Gasto de Belleza en verano(julio y agosto)

df1[(df1["SECTOR"]=="BELLEZA")].where("DIA BETWEEN '2015-07-01' AND '2015-09-01'").groupBy("DIA").sum("NUM_OP").orderBy("DIA").show(10000) 

#Comparativa de lo que gastan cada sector en el mes de diciembre en

df1.where("DIA BETWEEN '2015-12-01' AND '2015-12-31'").groupBy("SECTOR", "DIA").sum("NUM_OP").orderBy("SECTOR", "DIA").show(10000)
