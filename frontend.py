import numpy as np
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as plt
import pandas as pd
import datetime
from enum import Enum
import os
import json
from streamlit_folium import folium_static
import folium
import numpy as np

pages = {
  "main": "Inicio",
  "page1": "Mayores importes por localidad y sectores",
  "page2": "Importes en transacciones locales",
  "page3": "Ventas en Franjas Horarias",
  "page4": "Dias mas rentables del año",
}
  
selected_page = st.sidebar.radio("Selecciona la página", pages.values())

if selected_page == pages["main"]:
  st.write("Inicio")

elif selected_page == pages["page1"]:
  
  #Importe total por sectores

  datos = pd.read_csv('gs://bucket-prueba-nacho/importeTotalSector.csv', storage_options=({"token": "datos\grandes-volumenes-9d18b4ccbb2f.json"}))

  importe = datos['sum(IMPORTE)']
  sectores = datos['SECTOR']

  figura = plt.pie(names=sectores, values=importe, hole= 0.65, title="<b><i>Importe total por Sector</b></i>")
  figura.update_traces(textinfo='percent', textposition='outside', hovertemplate = "Sector %{label}: <br>Importe: %{value:.2f}</br> Porcentage: %{percent}")
  st.plotly_chart(figura)
  
  #Importes totales gastados en las localidades

  df = pd.read_csv('gs://bucket-prueba-nacho/importePorLocalidad.csv',sep = ',', storage_options=({"token": "datos\grandes-volumenes-9d18b4ccbb2f.json"}))
  df.drop(df.columns[[0]], axis=1, inplace=True)
  df['CP_CLIENTE'] = df['CP_CLIENTE'].apply(lambda x: '{0:0>5}'.format(x))

  components.html("<br><br><br><br><b><i>Importe total de los comercios con transacciones locales en cada sector</b></i>")

  with open('./datos/almeria_20.json') as f:
    states_topo = json.load(f)

  m = folium.Map(location=[37.16, -2.33], tiles='Stamen Terrain', zoom_start=9)

  folium.Choropleth(
      geo_data=states_topo,
      topojson='objects.almeria_wm',
      name="choropleth",
      line_color='white',
      line_weight=1,
      data=df,
      columns=["CP_CLIENTE", "sum(IMPORTE)"],
      key_on="feature.properties.COD_POSTAL",
      fill_color="YlOrRd",
      fill_opacity=0.6,
      line_opacity=0.4,
      legend_name="Importe total que gasta cada localidad",
  ).add_to(m)
  folium_static(m)

elif selected_page == pages["page2"]:

  #Importe total de los comercios con transacciones locales (todos los sectores juntos)

  importeLocal = pd.read_csv('gs://bucket-prueba-nacho/importeLocal.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  importe = importeLocal['sum(IMPORTE)']
  codPost = importeLocal['CP_COMERCIO']

  figLocal = plt.pie(names=codPost, values=importe, hole= 0.65, title="<b><i>Importes locales de las localidades</b></i>")
  figLocal.update_traces(textinfo='percent', textposition='outside', hovertemplate = "Codigo postal %{label}: <br>Importe: %{value:.2f}</br> Porcentage: %{percent}")
  st.plotly_chart(figLocal)

  #Importe total de los comercios con transacciones locales en cada sector

  localAlimentacion = pd.read_csv('gs://bucket-prueba-nacho/localAlimentacion.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  localAuto = pd.read_csv('gs://bucket-prueba-nacho/localAuto.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  localRestauracion = pd.read_csv('gs://bucket-prueba-nacho/localRestauracion.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  localBelleza = pd.read_csv('gs://bucket-prueba-nacho/localBelleza.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  localOtros = pd.read_csv('gs://bucket-prueba-nacho/localOtros.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  localOcio = pd.read_csv('gs://bucket-prueba-nacho/localOcio.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  localHogar = pd.read_csv('gs://bucket-prueba-nacho/localHogar.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  localSalud = pd.read_csv('gs://bucket-prueba-nacho/localSalud.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  localModa = pd.read_csv('gs://bucket-prueba-nacho/localModa.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  localTecnologia = pd.read_csv('gs://bucket-prueba-nacho/localTecnologia.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")

  lista=[]

  localAlimentacion=localAlimentacion.to_numpy().ravel().tolist()
  lista.append(localAlimentacion)
  localAuto=localAuto.to_numpy().ravel().tolist()
  lista.append(localAuto)
  localRestauracion=localRestauracion.to_numpy().ravel().tolist()
  lista.append(localRestauracion)
  localBelleza=localBelleza.to_numpy().ravel().tolist()
  lista.append(localBelleza)
  localOtros=localOtros.to_numpy().ravel().tolist()
  lista.append(localOtros)
  localOcio=localOcio.to_numpy().ravel().tolist()
  lista.append(localOcio)
  localHogar=localHogar.to_numpy().ravel().tolist()
  lista.append(localHogar)
  localSalud=localSalud.to_numpy().ravel().tolist()
  lista.append(localSalud)
  localModa=localModa.to_numpy().ravel().tolist()
  lista.append(localModa)
  localT=localTecnologia.append({'CP_COMERCIO' : 4002 , 'sum(IMPORTE)' : 0}, ignore_index=True).sort_values('CP_COMERCIO').drop(columns=['CP_COMERCIO'])
  localT=localT.to_numpy().ravel().tolist()
  lista.append(localT)

  components.html("<br><br><br><br><b><i>Importe total de los comercios con transacciones locales en cada sector</b></i>")
  fig = plt.imshow(lista,
                labels=dict(x="CODIGOS POSTALES", y="SECTOR", color="IMPORTE"),
                x=['4001', '4002', '4003', '4004', '4005', '4006', '4007', '4008', '4009'],
                y=['ALIMENTACION','AUTO', 'RESTAURACION','BELLEZA','OTROS','OCIO Y TIEMPO LIBRE','HOGAR','SALUD','MODA Y COMPLEMENTOS','TECNOLOGIA']
               )
  fig.update_xaxes(side="top")
  st.plotly_chart(fig)

elif selected_page == pages["page3"]:
  sector = st.selectbox("Seleccione un Sector", ['AUTO', 'ALIMENTACION', 'BELLEZA', 'HOGAR', 'MODA Y COMPLEMENTOS', 'OCIO Y TIEMPO LIBRE', 'OTROS', 'RESTAURACION', 'SALUD', 'TECNOLOGIA'])
  datos_importe = pd.read_csv('gs://bucket-prueba-nacho/datosSectorImporte.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  datos_grafica = datos_importe[datos_importe.SECTOR == sector]
  lista_franjas1 = []
  for i in range(len(datos_grafica.index)):
    franjas = ["0-2", "2-4", "4-6", "6-8", "8-10", "10-12", "12-14", "14-16", "16-18", "18-20", "20-22", "22-24"]
    lista_franjas1.append(franjas[i])
  
  importe = datos_grafica['sum(IMPORTE)']
  figura_imp = plt.bar(datos_grafica, x=lista_franjas1, y=importe)
  figura_imp.update_layout(title="<b><i>Importe TOTAL por Sector por Franja Horaria</b></i>",xaxis_title="Franja Horaria", yaxis_title="Importe", xaxis_type="category")
  figura_imp.update_traces(hovertemplate="Franja Horaria: %{x} <br> Importe: %{y}</br>")
  st.plotly_chart(figura_imp)
  
  datos_operaciones = pd.read_csv('gs://bucket-prueba-nacho/datosSectorOperaciones.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  datos_grafica_2 = datos_operaciones[datos_importe.SECTOR == sector]
  lista_franjas2 = []
  for i in range(len(datos_grafica_2.index)):
    franjas2 = ["0-2", "2-4", "4-6", "6-8", "8-10", "10-12", "12-14", "14-16", "16-18", "18-20", "20-22", "22-24"]
    lista_franjas2.append(franjas2[i])
  operaciones = datos_grafica_2['sum(NUM_OP)']
  figura_ops = plt.bar(datos_grafica_2, x=lista_franjas2, y=operaciones)
  figura_ops.update_layout(title="<b><i>Operaciones TOTALES por Sector por Franja Horaria</b></i>",xaxis_title="Franja Horaria", yaxis_title="Operaciones", xaxis_type="category")
  figura_ops.update_traces(hovertemplate="Franja Horaria: %{x} <br> Operaciones: %{y}</br>")
  st.plotly_chart(figura_ops)

elif selected_page == pages["page4"]:
  sector = st.selectbox("Seleccione un Sector", ['AUTO', 'ALIMENTACION', 'BELLEZA', 'HOGAR', 'MODA Y COMPLEMENTOS', 'OCIO Y TIEMPO LIBRE', 'OTROS', 'RESTAURACION', 'SALUD', 'TECNOLOGIA'])
  datos_dias_rent = pd.read_csv('.\datos\diasConMasOperaciones.csv',sep = ',')
  datos_sector = datos_dias_rent[datos_dias_rent.SECTOR == sector]

  dias_saturados = datos_sector['DIA']
  operaciones = datos_sector['sum(NUM_OP)']

  figura_ops = plt.bar(datos_sector, x=dias_saturados, y=operaciones)
  figura_ops.update_layout(title="<b><i>Top de dias con mayores ventas</b></i>",xaxis_title="Dias", yaxis_title="Operaciones", xaxis_type="category")
  figura_ops.update_traces(hovertemplate="Dia: %{x} <br> Operaciones: %{y}</br>")
  st.plotly_chart(figura_ops)
