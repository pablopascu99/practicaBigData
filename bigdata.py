from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd

sc = SparkContext('local')
spark = SparkSession(sc)

#Importe medio por sector

'''df1 = spark.read.csv("./PracticaBigData/datos/cards.csv",header=True,inferSchema=True,sep = '|',)
p = df1.toPandas()
p['IMPORTE'] = [x.replace(',', '.') for x in p['IMPORTE']]
p['IMPORTE'] = p['IMPORTE'].astype(float)
p.to_csv('./PracticaBigData/datos/cards1.csv')
df = spark.read.csv("./PracticaBigData/datos/cards1.csv",header=True,inferSchema=True,sep = ',',)
importeMedioSector = df.groupBy("SECTOR").mean( "IMPORTE") 
importeMedioSector.show()'''

#Codigos Postales

df2 = spark.read.csv("./PracticaBigData/datos/codigoPostalCoordenadas.csv",header=True,inferSchema=True,sep = ';')
df3 = spark.read.csv("./PracticaBigData/datos/weather.csv",header=True,inferSchema=True,sep = ';')
df = spark.read.csv("./PracticaBigData/datos/cards1.csv",header=True,inferSchema=True,sep = ',')
'''
importeCP = df.groupBy("CP_CLIENTE").sum("IMPORTE") 
i=importeCP.join(df2,importeCP.CP_CLIENTE == df2.codigopostalid,"left")
i.dropDuplicates(['CP_CLIENTE','codigopostalid']).show()'''

#Franja horaria con mas ventas

'''i=df.groupBy("FRANJA_HORARIA").sum("NUM_OP")
j=df.groupBy("FRANJA_HORARIA").sum("IMPORTE")
k=i.join(j,i.FRANJA_HORARIA == j.FRANJA_HORARIA, "left")
k.show(20)'''

#Franja horaria con mas ventas
m1=df[(df["SECTOR"]=="OCIO Y TIEMPO LIBRE")]
m=m1.groupBy("DIA").sum("NUM_OP") 
k=df3.join(m,df3.FECHA == m.DIA, "left").select(m["DIA"],df3["Precip"],m["sum(NUM_OP)"])
k.groupBy("DIA").sum("sum(NUM_OP)")
k1=k[(k["Precip"]>=5)]
k1.show(30)