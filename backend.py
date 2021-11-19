from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd
from google.cloud import storage

sc = SparkContext('local')
spark = SparkSession(sc)

#Importe medio por sector

df1 = spark.read.csv("./datos/cards.csv",header=True,inferSchema=True,sep = '|')
data = df1.toPandas()
data['IMPORTE'] = [x.replace(',', '.') for x in data['IMPORTE']]
data['IMPORTE'] = data['IMPORTE'].astype(float)
data.to_csv('gs://bucket-prueba-nacho/cardsNuevo.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))

df = spark.read.csv("./datos/cardsNuevo.csv",header=True,inferSchema=True,sep = ',')
importeMedioSector = df.groupBy("SECTOR").mean("IMPORTE")
importeMedioSector.toPandas().to_csv('gs://bucket-prueba-nacho/importeMedioSector.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))

#Codigos Postales

df2 = spark.read.csv("./datos/codigoPostalCoordenadas.csv",header=True,inferSchema=True,sep = ';')
df3 = spark.read.csv("./datos/weather.csv",header=True,inferSchema=True,sep = ';')

importeCP = df.groupBy("CP_CLIENTE").sum("IMPORTE") 
dfImporte = importeCP.join(df2,importeCP.CP_CLIENTE == df2.codigopostalid,"left")
dfImporte.dropDuplicates(['CP_CLIENTE','codigopostalid']).drop("CP_CLIENTE")
dfImporte.toPandas().dropna().to_csv('gs://bucket-prueba-nacho/importePorLocalidad.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
dfx = spark.read.csv("./datos/importePorLocalidad.csv",header=True,inferSchema=True,sep = ',')

#Franja horaria con mas ventas

i=df.groupBy("FRANJA_HORARIA").sum("NUM_OP")
j=df.groupBy("FRANJA_HORARIA").sum("IMPORTE")
k=i.join(j,i.FRANJA_HORARIA == j.FRANJA_HORARIA, "left")
k.show(20)

#Franja horaria con mas ventas
'''m1=df[(df["SECTOR"]=="OCIO Y TIEMPO LIBRE")]
m=m1.groupBy("DIA").sum("NUM_OP") 
k=df3.join(m,df3.FECHA == m.DIA, "left").select(m["DIA"],df3["Precip"],m["sum(NUM_OP)"])
k.groupBy("DIA").sum("sum(NUM_OP)")
k1=k[(k["Precip"]>=5)]
k1.show(30)'''