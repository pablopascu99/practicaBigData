from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd
from google.cloud import storage

sc = SparkContext('local')
spark = SparkSession(sc)

#Importe total por sector

data = pd.read_csv('gs://bucket-prueba-nacho/cards.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), sep= '|')
data['IMPORTE'] = [x.replace(',', '.') for x in data['IMPORTE']]
data['IMPORTE'] = data['IMPORTE'].astype(float)
data.to_csv('gs://bucket-prueba-nacho/cardsNuevo.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))


df = pd.read_csv("gs://bucket-prueba-nacho/cardsNuevo.csv", storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), sep = ',')
df = spark.createDataFrame(df)
df.show()
importeTotalSector = df.groupBy("SECTOR").sum("IMPORTE")
importeTotalSector.toPandas().to_csv('gs://bucket-prueba-nacho/importeTotalSector.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))

#Codigos Postales

df2 = pd.read_csv("gs://bucket-prueba-nacho/codigoPostalCoordenadas.csv",sep = ';', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
df3 = pd.read_csv("gs://bucket-prueba-nacho/weather.csv",sep = ';', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))

df2 = spark.createDataFrame(df2)
df3 = spark.createDataFrame(df3)

importeCP = df.groupBy("CP_CLIENTE").sum("IMPORTE") 
dfImporte = importeCP.join(df2,importeCP.CP_CLIENTE == df2.codigopostalid,"left")
dfImporte.dropDuplicates(['CP_CLIENTE','codigopostalid']).drop("CP_CLIENTE")
dfImporte.toPandas().dropna().to_csv('gs://bucket-prueba-nacho/importePorLocalidad.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))

#Franja horaria con mas ventas
total_importe = df.groupBy("SECTOR", "FRANJA_HORARIA").sum("IMPORTE")
total_ops = df.groupBy("SECTOR", "FRANJA_HORARIA").sum("NUM_OP")
datos_importe = total_importe.toPandas().sort_values(['SECTOR', 'FRANJA_HORARIA'])
datos_operaciones = total_ops.toPandas().sort_values(['SECTOR', 'FRANJA_HORARIA'])

datos_importe.to_csv('gs://bucket-prueba-nacho/datosSectorImporte.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
datos_operaciones.to_csv('gs://bucket-prueba-nacho/datosSectorOperaciones.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))

#Importe total de los comercios con transacciones locales (todos los sectores juntos)

importeLocal = df[(df["CP_CLIENTE"]==df["CP_COMERCIO"])].groupBy("CP_COMERCIO").sum("IMPORTE").toPandas()
importeLocal.to_csv('gs://bucket-prueba-nacho/importeLocal.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))

#Importe total de los comercios con transacciones locales en cada sector

alimentacion=df[df["CP_CLIENTE"]==df["CP_COMERCIO"]].filter((df.SECTOR  == "ALIMENTACION")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
alimentacion.to_csv('gs://bucket-prueba-nacho/localAlimentacion.csv', index=False, storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
auto=df[df["CP_CLIENTE"]==df["CP_COMERCIO"]].filter((df.SECTOR  == "AUTO")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
auto.to_csv('gs://bucket-prueba-nacho/localAuto.csv', index=False, storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
restauracion=df[df["CP_CLIENTE"]==df["CP_COMERCIO"]].filter((df.SECTOR  == "RESTAURACION")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
restauracion.to_csv('gs://bucket-prueba-nacho/localRestauracion.csv', index=False, storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
belleza=df[df["CP_CLIENTE"]==df["CP_COMERCIO"]].filter((df.SECTOR  == "BELLEZA")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
belleza.to_csv('gs://bucket-prueba-nacho/localBelleza.csv', index=False, storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
otros=df[df["CP_CLIENTE"]==df["CP_COMERCIO"]].filter((df.SECTOR  == "OTROS")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
otros.to_csv('gs://bucket-prueba-nacho/localOtros.csv', index=False, storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
ocio=df[df["CP_CLIENTE"]==df["CP_COMERCIO"]].filter((df.SECTOR  == "OCIO Y TIEMPO LIBRE")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
ocio.to_csv('gs://bucket-prueba-nacho/localOcio.csv', index=False, storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
hogar=df[df["CP_CLIENTE"]==df["CP_COMERCIO"]].filter((df.SECTOR  == "HOGAR")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
hogar.to_csv('gs://bucket-prueba-nacho/localHogar.csv', index=False, storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
salud=df[df["CP_CLIENTE"]==df["CP_COMERCIO"]].filter((df.SECTOR  == "SALUD")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
salud.to_csv('gs://bucket-prueba-nacho/localSalud.csv', index=False, storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
moda=df[df["CP_CLIENTE"]==df["CP_COMERCIO"]].filter((df.SECTOR  == "MODA Y COMPLEMENTOS")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
moda.to_csv('gs://bucket-prueba-nacho/localModa.csv', index=False, storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))
tecno=df[df["CP_CLIENTE"]==df["CP_COMERCIO"]].filter((df.SECTOR  == "TECNOLOGIA")).groupBy("CP_COMERCIO").sum("IMPORTE").toPandas().sort_values('CP_COMERCIO')
tecno.to_csv('gs://bucket-prueba-nacho/localTecnologia.csv', index=False, storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))

#Franja horaria con mas ventas
'''m1=df[(df["SECTOR"]=="OCIO Y TIEMPO LIBRE")]
m=m1.groupBy("DIA").sum("NUM_OP") 
k=df3.join(m,df3.FECHA == m.DIA, "left").select(m["DIA"],df3["Precip"],m["sum(NUM_OP)"])
k.groupBy("DIA").sum("sum(NUM_OP)")
k1=k[(k["Precip"]>=5)]
k1.show(30)'''

#Top de 10 Dias con mayor numero de transaciones
# Segun cada sector
ali=df[(df["SECTOR"]=="ALIMENTACION")].groupBy("SECTOR","DIA").sum("NUM_OP").sort("sum(NUM_OP)",ascending=False).limit(10)
hog=df[(df["SECTOR"]=="HOGAR")].groupBy("SECTOR","DIA").sum("NUM_OP").sort("sum(NUM_OP)",ascending=False).limit(10)
res=df[(df["SECTOR"]=="RESTAURACION")].groupBy("SECTOR","DIA").sum("NUM_OP").sort("sum(NUM_OP)",ascending=False).limit(10)
aut=df[(df["SECTOR"]=="AUTO")].groupBy("SECTOR","DIA").sum("NUM_OP").sort("sum(NUM_OP)",ascending=False).limit(10)
otr=df[(df["SECTOR"]=="OTROS")].groupBy("SECTOR","DIA").sum("NUM_OP").sort("sum(NUM_OP)",ascending=False).limit(10)
sal=df[(df["SECTOR"]=="SALUD")].groupBy("SECTOR","DIA").sum("NUM_OP").sort("sum(NUM_OP)",ascending=False).limit(10)
ocio=df[(df["SECTOR"]=="OCIO Y TIEMPO LIBRE")].groupBy("SECTOR","DIA").sum("NUM_OP").sort("sum(NUM_OP)",ascending=False).limit(10)
mod=df[(df["SECTOR"]=="MODA Y COMPLEMENTOS")].groupBy("SECTOR","DIA").sum("NUM_OP").sort("sum(NUM_OP)",ascending=False).limit(10)
tec=df[(df["SECTOR"]=="TECNOLOGIA")].groupBy("SECTOR","DIA").sum("NUM_OP").sort("sum(NUM_OP)",ascending=False).limit(10)
bel=df[(df["SECTOR"]=="BELLEZA")].groupBy("SECTOR","DIA").sum("NUM_OP").sort("sum(NUM_OP)",ascending=False).limit(10)
total=hog.union(ali).union(res).union(aut).union(otr).union(sal).union(ocio).union(mod).union(tec).union(bel).sort("SECTOR")
total.toPandas().to_csv('gs://bucket-prueba-nacho/diasConMasOperaciones.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))

#Top 10 dias con mayor saturacion entre todos los sectores
li=["2015-01-03","2015-01-05","2015-01-02","2015-12-23","2015-12-30","2015-11-27","2015-07-01","2015-12-29","2015-11-28","2015-12-22"]

m=df.filter(df.DIA.isin(li)).groupBy("DIA","FRANJA_HORARIA").sum("NUM_OP")
n=m.orderBy("DIA", "FRANJA_HORARIA")
n.toPandas().to_csv('gs://bucket-prueba-nacho/diasConMasOperTotal.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}))