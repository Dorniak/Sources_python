import os.path
from pyspark import SparkContext

def carga(nombre, sc):
    mortalidad = os.path.join('../CSV', nombre)
    return sc.textFile(mortalidad)

def enRango(anio,inicio,final):
    if final>anio>inicio:
        return True
    else:
        return False

def filtro(rdd,inicio,final):
    return rdd.filter(lambda linea:enRango(str(linea).split(',')[8].split('-')[0],inicio,final))

sc=SparkContext(appName='proyecto1')
rdd_mortalidad = carga('mortalidad.csv',sc)
#rdd_resultado = filtro(rdd_mortalidad,'2000','2003')
#print rdd_resultado.count()
print rdd_mortalidad.take(1)
