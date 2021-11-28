
from pandas.core.indexing import convert_from_missing_indexer_tuple
from pyspark.sql.functions import sequence, to_date, explode, col
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import sys
import plotly.express as px

sc = SparkContext('local')
spark = SparkSession(sc)

df2 = spark.read.csv("C:\Program Files\Spark\PracticaBigData\datos\codigoPostalCoordenadas.csv",header=True,inferSchema=True,sep = ';')
df3 = spark.read.csv("C:\Program Files\Spark\PracticaBigData\datos\weather.csv",header=True,inferSchema=True,sep = ';')
df1 = spark.read.csv("C:\Program Files\Spark\PracticaBigData\datos\cardsNuevo.csv",header=True,inferSchema=True,sep = ',')


print(df1[(df1["CP_CLIENTE"]==df1["CP_COMERCIO"])].groupBy("CP_COMERCIO").sum("IMPORTE").toPandas())
'''lista=[]

alimentacion=df1[df1["CP_CLIENTE"]==df1["CP_COMERCIO"]].filter((df1.SECTOR  == "ALIMENTACION")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
alimentacion=alimentacion.to_numpy().ravel().tolist()
lista.append(alimentacion)

auto=df1[df1["CP_CLIENTE"]==df1["CP_COMERCIO"]].filter((df1.SECTOR  == "AUTO")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
auto=auto.to_numpy().ravel().tolist()
lista.append(auto)

restauracion=df1[df1["CP_CLIENTE"]==df1["CP_COMERCIO"]].filter((df1.SECTOR  == "RESTAURACION")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
restauracion=restauracion.to_numpy().ravel().tolist()
lista.append(restauracion)

belleza=df1[df1["CP_CLIENTE"]==df1["CP_COMERCIO"]].filter((df1.SECTOR  == "BELLEZA")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
belleza=belleza.to_numpy().ravel().tolist()
lista.append(belleza)

otros=df1[df1["CP_CLIENTE"]==df1["CP_COMERCIO"]].filter((df1.SECTOR  == "OTROS")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
otros=otros.to_numpy().ravel().tolist()
lista.append(otros)

ocio=df1[df1["CP_CLIENTE"]==df1["CP_COMERCIO"]].filter((df1.SECTOR  == "OCIO Y TIEMPO LIBRE")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
ocio=ocio.to_numpy().ravel().tolist()
lista.append(ocio)

hogar=df1[df1["CP_CLIENTE"]==df1["CP_COMERCIO"]].filter((df1.SECTOR  == "HOGAR")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
hogar=hogar.to_numpy().ravel().tolist()
lista.append(hogar)

salud=df1[df1["CP_CLIENTE"]==df1["CP_COMERCIO"]].filter((df1.SECTOR  == "SALUD")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
salud=salud.to_numpy().ravel().tolist()
lista.append(salud)

moda=df1[df1["CP_CLIENTE"]==df1["CP_COMERCIO"]].filter((df1.SECTOR  == "MODA Y COMPLEMENTOS")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
moda=moda.to_numpy().ravel().tolist()
lista.append(moda)
tecno=df1[df1["CP_CLIENTE"]==df1["CP_COMERCIO"]].filter((df1.SECTOR  == "TECNOLOGIA")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO')
t=tecno.append({'CP_COMERCIO' : 4002 , 'sum(IMPORTE)' : 0}, ignore_index=True).sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
t=t.to_numpy().ravel().tolist()
lista.append(t)

fig = px.imshow(lista,
                labels=dict(x="Day of Week", y="SECTOR", color="IMPORTE"),
                x=['4001', '4002', '4003', '4004', '4005', '4006', '4007', '4008', '4009'],
                y=['AUTO', 'RESTAURACION','BELLEZA','OTROS','OCIO Y TIEMPO LIBRE','HOGAR','SALUD','MODA Y COMPLEMENTOS','TECNOLOGIA']
               )
fig.update_xaxes(side="top")
fig.show()
fig2 = px.imshow(lista,
                labels=dict(x="Day of Week", y="SECTOR", color="IMPORTE"),
                x=['4001', '4002', '4003', '4004', '4005', '4006', '4007', '4008', '4009'],
                y=['ALIMENTACION','AUTO', 'RESTAURACION','BELLEZA','OTROS','OCIO Y TIEMPO LIBRE','HOGAR','SALUD','MODA Y COMPLEMENTOS']
               )
fig2.update_xaxes(side="top")
fig2.show()'''
