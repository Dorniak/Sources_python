# -*- coding: utf-8 -*-
import os.path
import matplotlib.pyplot as plt
#import numpy as np
from pyspark import SparkContext
#Carga de fichero csv
def carga(nombre, sc):
    contaminacion = os.path.join('../CSV', nombre)
    return sc.textFile(contaminacion)
#Comprobacion deque el año esta en rango
def enRango(anio,inicio,final):
    if final >= anio >= inicio:
        return True
    else:
        return False
#Filtrar por rango de años
def filtro(rdd,inicio,final):
    return rdd.filter(lambda linea:enRango(str(linea).split(',')[8].split('-')[0],inicio,final))
#Filtro de año introducido por consola
def filtro_anio():
    print ('Introducir año de inicio')
    anio_inicio = leer_consola()
    print ('Introducir año de final')
    anio_final = leer_consola()
    if not check_anios(anio_inicio,anio_final):
        raise Exception('Error en la entrada de años')
    return filtro(rdd_contaminacion, anio_inicio, anio_final)

#Obtener media NO2 en conjunto de años
def media_anio_NO2(anio=None, anio_final=None):
    if not anio and not anio_final:
        rdd_anio = filtro_anio()
        rdd_resultado = rdd_anio.map(lambda linea:float(str(linea).split(',')[10]))
        media = rdd_resultado.mean()
        return media
    elif anio and not anio_final:
        rdd_anio = filtro(rdd_contaminacion, anio, anio)
        rdd_resultado = rdd_anio.map(lambda linea: float(str(linea).split(',')[10]))
        media = rdd_resultado.mean()
        return media
    else:
        rdd_anio = filtro(rdd_contaminacion, anio, anio_final)
        rdd_resultado = rdd_anio.map(lambda linea: float(str(linea).split(',')[10]))
        media = rdd_resultado.mean()
        return media

#Obtener media O3 en conjunto de años
def media_anio_O3(anio=None,anio_final=None):
    if not anio and not anio_final:
        rdd_anio = filtro_anio()
        rdd_resultado = rdd_anio.map(lambda linea:float(str(linea).split(',')[15]))
        media = rdd_resultado.mean()
        return media
    elif anio and not anio_final:
        rdd_anio = filtro(rdd_contaminacion, anio, anio)
        rdd_resultado = rdd_anio.map(lambda linea: float(str(linea).split(',')[15]))
        media = rdd_resultado.mean()
        return media
    else:
        rdd_anio = filtro(rdd_contaminacion, anio, anio_final)
        rdd_resultado = rdd_anio.map(lambda linea: float(str(linea).split(',')[15]))
        media = rdd_resultado.mean()
        return media
#Obtener media SO2 en conjunto de años
def media_anio_SO2(anio=None,anio_final=None):
    if not anio and not anio_final:
        rdd_anio = filtro_anio()
        rdd_resultado = rdd_anio.map(lambda linea:float(str(linea).split(',')[20]))
        media = rdd_resultado.mean()
        return media
    elif anio and not anio_final:
        rdd_anio = filtro(rdd_contaminacion, anio, anio)
        rdd_resultado = rdd_anio.map(lambda linea: float(str(linea).split(',')[20]))
        media = rdd_resultado.mean()
        return media
    else:
        rdd_anio = filtro(rdd_contaminacion, anio, anio_final)
        rdd_resultado = rdd_anio.map(lambda linea: float(str(linea).split(',')[20]))
        media = rdd_resultado.mean()
        return media
#Obtener media CO en conjunto de años
def media_anio_CO(anio=None,anio_final=None):
    if not anio and not anio_final:
        rdd_anio = filtro_anio()
        rdd_resultado = rdd_anio.map(lambda linea:float(str(linea).split(',')[25]))
        media = rdd_resultado.mean()
        return media
    elif anio and not anio_final:
        rdd_anio = filtro(rdd_contaminacion, anio, anio)
        rdd_resultado = rdd_anio.map(lambda linea: float(str(linea).split(',')[25]))
        media = rdd_resultado.mean()
        return media
    else:
        rdd_anio = filtro(rdd_contaminacion, anio, anio)
        rdd_resultado = rdd_anio.map(lambda linea: float(str(linea).split(',')[25]))
        media = rdd_resultado.mean()
        return media

def mapper(linea):
    #rdd_valor = sc.parallelize(float(str(linea[1]).split(',')))
    print linea
    print tuple(linea)[0]
    print tuple(linea)[1]
    lista = [str(tuple(linea)[1]).split(',')]

    #rdd_valor = sc.parallelize(str(tuple(linea)[1]).split(',')).map(lambda x: float(x))
    #print rdd_valor.count()
    #print rdd_valor.mean()
    #return (tuple(linea)[0],rdd_valor.mean())
    return 0

def media_ciudad_anio_NO2():
    rdd_anio = filtro_anio()
    rdd_c_v = rdd_anio.map(lambda linea: (str(linea).split(',')[5],str(linea).split(',')[10]))
    rdd_ciudad = rdd_c_v.reduceByKey(lambda x,y: x+","+y )
    #print sc.parallelize(rdd_ciudad.take(1)[0][1].split(',')).map(lambda x:float(x)).mean()
    #rdd_ciudad2 = rdd_ciudad.map(lambda linea: (tuple(linea)[0] , sc.parallelize(str(tuple(linea)[1]).split(',')).map(lambda x:float(x)).mean()))

    rdd_ciudad2 = rdd_ciudad.map(mapper)

    #rdd_valor = sc.parallelize(str(rdd_ciudad.take(3)[0][1]).split(','))
    print rdd_ciudad2.take(1)
    #print rdd_ciudad.take(1)[0][1]
    #rdd_ciudad = rdd_ciudad.map(lambda linea: sum(linea[1])/len(linea[1]))

def prueba():
    #media por año en rango de años
    array_anios=[]
    array_NO2=[]
    array_O3 = []
    array_SO2 = []
    array_CO = []
    for i in range(2000,2005,1):
        array_anios.append(i)
        array_NO2.append(media_anio_NO2(str(i)))
        array_O3.append(float(media_anio_O3(str(i)))*1000)
        array_SO2.append(media_anio_SO2(str(i)))
        array_CO.append(media_anio_CO(str(i)))
    plt.ylim(0,25)
    plt.xlim(2000, 2016)
    plt.step(array_anios, array_NO2, where='mid', color='r', linewidth=3)
    plt.step(array_anios, array_O3, where='mid', color='g', linewidth=3)
    plt.step(array_anios, array_SO2, where='mid', color='b', linewidth=3)
    plt.step(array_anios, array_CO, where='mid', color='b', linewidth=3)#Cmabiar el color
    plt.show()

def print_menu():
    print ("Seleccionar modo trabajo:")
    print ("    1:Media por año NO2")
    print ("    2:Media por año O3")
    print ("    3:Media por año SO2")
    print ("    4:Media por año CO")
    print ("    5:Media por ciudad y año NO2 (No funciona)")
    print ("    6:pruebas")
    print ("    exit:salir")

def leer_consola():
    global contador_auto
    if automatico and contador_auto < auto.count():
        contador_auto = contador_auto+1
        return auto[contador_auto-1]
    else:
        return str(raw_input())

def check_anios(inicio,final):
    if 2000 <= int(inicio) <= 2016 and int(inicio) <= int(final) <= 2016:
        return True
    else:
        return False

sc=SparkContext(appName='proyecto1')
rdd_contaminacion = carga('pollution.csv',sc)

auto = ['1','2000','2000']
contador_auto = 0
automatico = False#auto=True,manual=False
while True:
    print ("")
    print_menu()
    print ("Introducir")
    numero = leer_consola()
    if numero == "exit":
        exit()
    elif numero == '1':
        try:
            resultado = media_anio_NO2()
            print('Media NO2: ' + str(resultado))
            automatico=False
        except:
            print ("Ha ocurrido una excepcion")
            exit()
    elif numero == '2':
        try:
            resultado = media_anio_O3()
            print('Media O3: ' + str(resultado))
            automatico = False
        except:
            exit()
    elif numero == '3':
        try:
            resultado = media_anio_SO2()
            print('Media SO2: ' + str(resultado))
            automatico = False
        except:
            exit()
    elif numero == '4':
        try:
            resultado = media_anio_CO()
            print('Media CO: ' + str(resultado))
            automatico = False
        except:
            exit()
    elif numero == '5':
        try:
            media_ciudad_anio_NO2()
            automatico = False
        except:
            exit()
    elif numero == '6':
        prueba()
        automatico = False
    else:
        print ("Ha introducido un numero invalido")
        print ("")
        automatico = False
        continue
#rdd_resultado = filtro_anio()#Consola
#rdd_resultado = filtro(rdd_contaminacion,'2000','2003')#Fijo
#print rdd_resultado.count()
#print rdd_contaminacion.take(1) #Coger informacion columnas
#State Code	County Code	Site Num	Address	State	County	City	Date Local	NO2 Units	NO2 Mean	NO2 1st Max Value	NO2 1st Max Hour	NO2 AQI	O3 Units	O3 Mean	O3 1st Max Value	O3 1st Max Hour	O3 AQI	SO2 Units	SO2 Mean	SO2 1st Max Value	SO2 1st Max Hour	SO2 AQI	CO Units	CO Mean	CO 1st Max Value	CO 1st Max Hour	CO AQI
