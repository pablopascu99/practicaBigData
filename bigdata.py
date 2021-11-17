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
df2.printSchema()

df = spark.read.csv("./PracticaBigData/datos/cards1.csv",header=True,inferSchema=True,sep = ',')
importeCP = df.groupBy("CP_CLIENTE").sum("IMPORTE") 
importeCP.printSchema()
'''importeCP.join(df2, importeCP("CP_CLIENTE") === df2("codigopostalid"))
importeCP.show()'''
i=importeCP.join(df2,importeCP.CP_CLIENTE == df2.codigopostalid,"left")
i.show()
i.dropDuplicates(['CP_CLIENTE','codigopostalid']).show()