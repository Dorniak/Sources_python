# -*- coding: utf-8 -*-
import os.path
from pyspark import SparkContext

def carga(nombre, sc):
    contaminacion = os.path.join('../CSV', nombre)
    return sc.textFile(contaminacion)

def enRango(anio,inicio,final):
    if final>=anio>=inicio:
        return True
    else:
        return False

def filtro(rdd,inicio,final):
    return rdd.filter(lambda linea:enRango(str(linea).split(',')[8].split('-')[0],inicio,final))

def filtro_anio():
    print ('Introducir año de inicio')
    anio_inicio = str(input())
    print ('Introducir año de final')
    anio_final = str(input())
    return filtro(rdd_contaminacion, anio_inicio, anio_final)

def prueba(rdd):
    print ('prueb')
    print str(rdd).split(',')[10]
    return True

def media_anio_NO2():
    rdd_anio = filtro_anio()
    rdd_resultado = rdd_anio.map(lambda linea:float(str(linea).split(',')[10]))
    media = rdd_resultado.mean()
    print media

sc=SparkContext(appName='proyecto1')
rdd_contaminacion = carga('pollution.csv',sc)

media_anio_NO2()

#rdd_resultado = filtro_anio()#Consola
#rdd_resultado = filtro(rdd_contaminacion,'2000','2003')#Fijo
#print rdd_resultado.count()
#print rdd_contaminacion.take(1) #Coger informacion columnas
#State Code	County Code	Site Num	Address	State	County	City	Date Local	NO2 Units	NO2 Mean	NO2 1st Max Value	NO2 1st Max Hour	NO2 AQI	O3 Units	O3 Mean	O3 1st Max Value	O3 1st Max Hour	O3 AQI	SO2 Units	SO2 Mean	SO2 1st Max Value	SO2 1st Max Hour	SO2 AQI	CO Units	CO Mean	CO 1st Max Value	CO 1st Max Hour	CO AQI
