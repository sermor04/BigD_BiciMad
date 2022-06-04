#!/usr/bin/env python
# coding: utf-8

print('\n PRÁCTICA BICIMAD \n')
# 
print('Práctica realizada por Ana Sanuy Garabatos, Sergio Moreno González y Tomás Vela Angulo. \n')
# 
print('Problema a resolver\n')
# 
print('En esta práctica se van a utilizar las bases de datos 202106.json y 202106_movements.json, ofrecidas por la empresa de alquiler de bicicletas en Madrid, Bicimad.\n')
# 
print('La motivación de este proyecto es realizar un análisis de datos y responder algunas preguntas sobre los usuarios de la aplicación y los viajes que han realizado, que podrían ser útiles e interesantes a la hora de querer evaluar cómo va la empresa, tener información para futuras mejoras de la misma o para la posible detección de pérdidas, etc. Estas preguntas se plantearán después de una breve visualización de cómo son las dos bases de datos y se fijarán como nuestro objetivo.\n')
# 
print('Para ello, se utilizarán las técnicas de procesamiento masivo de datos aprendidas en la asignatura de Big Data, utilizando Spark, impartida por el profesor Luis Llana.\n')
# 
print('Reducimos el tamaño de los collect a 20 para verlos en la terminal ya que algunos son consultas muy grandes. En el notebook se podrán ver enteros.  \n')

print('IMPORTANTE: Cerrar la pestaña donde se muestran los gráficos una vez examinados para que la práctica siga funcionando. \n')
#Introducción

# Como hemos comentado, vamos a utilizar Spark, por tanto en primer lugar, nos conectamos a Spark e importamos las librerías que vamos a utilizar a lo largo de la práctica.

# In[1]:


# Spark
#====================================================
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

conf1 = SparkConf().setAppName("Airbnb_AnSeTo")
sc = SparkContext(conf = conf1)

# Operadores
#====================================================
import json
from functools import reduce
from operator import add

# Fechas y horas
#====================================================
from datetime import datetime
from datetime import timezone

# Visualización
#====================================================
#pip install matplotlib
import matplotlib.pyplot as plt

import pprint
from pprint import pprint as p

# Creamos la función spark para leer los datos como dataframes y observar mejor su estructura.

# In[2]:


spark = SparkSession.builder     .master("local[1]")     .appName("Airbnb_AnSeTo")     .getOrCreate()


print('\n Dataset 202106.json\n')

print('Es una base de datos en la que están recogidas cada hora de cada día del mes de junio de 2021, y en cada una de esas horas se recogen los datos de las 264 estaciones de bicicletas de la empresa Bicimad\n')



print('Los datos que se recogen de cada estación en cada hora del día son:activate,name,reservations_count,light,total_bases,free_bases,number,longitude,no_available,address,latitude,dock_bikes,id \n')

# Observémoslo como dataframe para mayor claridad.

# In[5]:


spark = SparkSession.builder.getOrCreate()


# In[6]:


df = spark.read.json('202106.json')


# In[7]:


df.printSchema()
df.show()
df.count()



print(' Viendo la estructura de la base de datos y cómo es cada una de las entradas, se nos ocurren las siguientes preguntas que pueden ser interesantes intentar responder: \n')
print('1. ¿Cuál es el número medio de estaciones por hora? De esas, ¿ha habido alguna hora en la que haya habido alguna estación inhabilitada? Si las ha habido, ¿cuántas y cuáles han sido? \n')
print('2. ¿Cuántas bicicletas entran en la estación de Miguel Moya? ¿Cuántas bicicletas ha tenido aparcadas por hora? ¿En qué horas ha estado completamente vacía?\n')
print( '3. ¿Qué estaciones han tenido bases inhabilitadas? ¿Cuántas han tenido y en qué fechas? \n')
print( '4. ¿Cuántas bases han tenido entre a e b bicicletas aparcadas? ¿Cuáles son y a qué horas han sido? \n')
print( '5. ¿Cual es el porcentaje de estaciones que han tenido bases reservadas? \n')
print( '6. ¿Se ha llenado alguna estación completamente? ¿Cuáles y cuantas? \n')
print( '7. Consideremos una base muy utilizada aquella que ha tenido más del 80% de sus huecos vacíos en algún momento. ¿Cuáles son? ¿Cuándo se han llenado completamente? \n')
print( '8. Histograma de la cantidad de bicicletas que se han aparcado por estación. \n')

# ### 0. Organización de los RDD
# Vamos a crear las funciones necesarias para leer bien los RDD y poder hacer las consultas propuestas. Vemos que cada id tiene asociada todas las estaciones con sus respectivos datos. Eso es un poco tedioso y dificulta el trabajo a la hora realizar las consultas. Lo que queremos será que cada id recoja una estación y ese id se repita tantas veces como estaciones tenga registradas.

# In[8]:


rdd_base_1 = sc.textFile('202106.json')


# In[9]:


def mapper(line):
    data = json.loads(line)
    station = []
    for i in data['stations']:
        _id = data['_id']
        trips = i
        station.append((_id, trips))
    return station


# Utilizamos flatMap en vez de map para sacar los valores de la lista.

# In[10]:


rdd = rdd_base_1.flatMap(mapper)


# In[11]:

print('veamos tres observaciones, para entender la distribución de los datos\n')
p(rdd.take(3))


print('\n Ahora que ya está organizado como deseábamos podemos comenzar a responder las preguntas propuestas.\n')


print('1. ¿Cuál es el número medio de estaciones por hora? De esas, ¿ha habido alguna hora en la que haya habido alguna estación inhabilitada? Si las ha habido, ¿cuántas y cuáles han sido?\n')

# Gracias al mapeo que hemos realizado, la primera pregunta se resuelve fácilmente con un countByKey y luego sumando los valores que nos de y dividiendo entre el número de valores diferentes que haya.

# In[12]:

p(rdd.countByKey())


print('\n Vaya, observemos que directamente, todas las horas registran el mismo número de estaciones por lo que la media va a ser directamente 264. Y nos ahorramos las operaciones que hemos dicho antes.','Para responder la segunda pregunta, tendremos que acceder a los valores de activate y comprobar si son 1 (activada) o 0 (desactivada). Hagamos un map primero para simplificar la información, cogiendo sólo si la estación está activada o no.\n')

# In[13]:

rdd_active = rdd.map(lambda x: x[1]['activate'])


# In[14]:


p(rdd_active.take(10))


# In[15]:


print('\n',rdd_active.countByValue(),'\n')


print('Como no hay ningún valor diferente a 1, podemos concluir que las estaciones siempre han estado activas durante el mes de junio de 2021. \n')



print(' 2. ¿Cuántas bicicletas entran en la estación de Miguel Moya? ¿Cuántas bicicletas ha tenido aparcadas por hora? ¿En qué horas ha estado completamente vacía?\n')

print('Queremos únicamente la información de la estación Miguel Moya y, de hecho, sólo las bases que tiene disponible. Bastará tomar el valor del RDD correspondiente a Miguel Moya (3) e irnos a la información que buscamos.\n')

# In[16]:


p(rdd.take(3)[2][1]['total_bases'])


print('\n Saber cuántas bicicletas ha tenido aparcadas por hora es algo más complejo, pero con un filter que coja la estación que queremos, un map que simplifique la información y un collect lo podemos obtener.\n')

# In[17]:


name = 'Miguel Moya'
rdd_bikes = rdd.filter(lambda x: x[1]['name'] == name).map(lambda x: (x[0], x[1]['dock_bikes']))


# In[18]:


p(rdd_bikes.take(20))


print('\n Para saber en qué horas ha estado vacía, haremos un filter con dock_bikes = 0 al rdd anterior y con un map, devolvemos sólo la hora.\n')

# In[19]:


rdd_empty = rdd_bikes.filter(lambda x: x[1]==0).map(lambda x: x[0])


# In[20]:


p(rdd_empty.take(20))


print('\n 3. ¿Qué estaciones han tenido bases inhabilitadas? ¿Cuántas han sido? ¿Cuántas bases han tenido rotas por fecha? \n')
print('Observemos que una estación ha tenido bases inhabilitadas si la suma entre reservas, bicis aparcadas y huecos libres es menor que la cantidad de bases disponibles en la estación. Usaremos claramente un filter para saber cuáles han estado inhabilitadas junto con la función distinct para que no se repitan.\n')

# In[21]:


p(rdd.take(1)[0][1])


# In[22]:


rdd_broken = rdd.filter(lambda x: (x[1]['total_bases'] > x[1]['free_bases'] + x[1]['reservations_count'] + 
                                   x[1]['dock_bikes'])).map(lambda x: x[1]['name']).distinct()


# In[23]:


print('\n',rdd_broken.collect(),'\n')


print('Para saber cuantas son basta hacer un count. \n')

# In[24]:


print(rdd_broken.count(),'\n')


print(' Wow, sólo ha habido 4 estaciones que nunca han tenido ninguna base rota.\n')
# 
print(' Vamos a ver cuándo éstas estaciones han tenido las bases rotas y cuántas tenían. Tendremos que organizar bien los datos con un map para luego hacer un groupByKey que los ordene. \n')




rdd_date_broken = rdd.filter(lambda x: (x[1]['total_bases'] > x[1]['free_bases'] + x[1]['reservations_count'] + 
                                   x[1]['dock_bikes'])).map(lambda x: (x[1]['name'], [x[0], x[1]['total_bases']-
                                                                       x[1]['free_bases']-x[1]['reservations_count']
                                                                       -x[1]['dock_bikes']])).groupByKey().mapValues(tuple)


print(' Hacemos un collect (reducido a dos estaciones por su gran extensión) y tenemos el resultado buscado. \n')

# In[26]:


p(rdd_date_broken.take(2))


print(' Fijémonos en lo útil que es esta consulta, pues de un vistazo podemos saber cuándo se estropeó una base y cuándo fueron a arreglar las bases de cada estación. \n')




print('4. ¿Cuántas bases han tenido entre a y b bicicletas aparcadas? ¿Cuáles son y a qué horas han sido?\n')

# In[27]:


p(rdd.take(1))


print('\n Tomemos a=1 y b=2 para poder ver resultados, aunque el rdd que vamos a crear va a poder funcionar para cualquier valor de a y b que pongamos. Necesitaremos un filter, un map y la función distinct para que no se repitan las bases continuamente.\n')

# In[28]:


a=1
b=2
rdd_dock = rdd.filter(lambda x: a <= (x[1]['dock_bikes']) <=b).map(lambda x: x[1]['name']).distinct()


# In[29]:


p(rdd_dock.take(20))


# In[30]:


print('\n','Hay: ',rdd_dock.count(),'\n')


print(' Tomando un intervalo de [1,2], vemos cómo solo 11 estaciones no han tenido entre una o dos bicis aparcadas únicamente a una hora concreta. Veamos ahora cúando ha sido esto: \n')

# In[31]:


rdd_date_dock = rdd.filter(lambda x: a <= (x[1]['dock_bikes']) <=b).map(lambda x: (x[1]['name'],[x[0],x[1]['dock_bikes']])).groupByKey().mapValues(tuple)


# In[32]:


p(rdd_date_dock.take(5))


print('\n Con esto, fijando un a y un b, podremos ver a qué hora había el número de bicicletas aparcadas que buscabamos en las estaciones. Vamos a comprobar ahora en cuales no había bicicletas disponibles. \n')

# In[33]:


rdd_dock_0 = rdd.filter(lambda x: (x[1]['dock_bikes']) == 0).map(lambda x: x[1]['name']).distinct()


# In[34]:


p(rdd_dock_0.take(20))


# In[35]:


print('\n','Hay: ',rdd_dock_0.count(),'\n')


print('Con esto, podemos ver cómo 264 - 202 = 62 estaciones no han estado vacías en ningun momento. Esta información es muy interesante para poder considerar qué estaciones podrían necesitar una ampliación. Por último veamos las fechas en las que no han tenido bicicletas disponibles y así poder saber cuáles son las horas y días de más uso de cada estación. \n')

# In[36]:


rdd_date_dock_0 = rdd.filter(lambda x: a <= (x[1]['dock_bikes']) <=b).map(lambda x: (x[1]['name'],[x[0],x[1]['dock_bikes']])).groupByKey().mapValues(tuple)


# In[37]:


p(rdd_date_dock_0.take(5))


print('\n 5. ¿Cual es el porcentaje de estaciones que han tenido bases reservadas?\n')

print('Hagamos un map primero para simplificar la información, cogiendo sólo si la estación ha sido reservada o no.\n')

# In[38]:


rdd_reservations = rdd.map(lambda x: x[1]['reservations_count'])


print('\n Hagamos un countByValue para ver si hay algún valor distinto de 0 que nos indique que alguna base ha estado reservada, y cuántas veces \n')

# In[39]:


print(rdd_reservations.countByValue(),'\n')


print('Vemos que sí que ha habido reservas (si no serían todos 0)\n')

print(' Veamos cuántas estaciones han tenido bases reservadas.\n')

# In[40]:


rdd_reservations1=rdd.filter(lambda x: x[1]['reservations_count']>0).map(lambda x: x[1]['name']).distinct()


# In[41]:


print(rdd_reservations1.count(),'\n')


print(' Pasemoslo a porcentaje.\n')

# In[42]:


print(rdd_reservations1.count()*100/264,'\n')


print(' Entonces, casi el 98% de estaciones han tenido bases reservadas \n')

print(' Como extra, para poder ver la información cómoda de un vistazo, recojamos los datos de cada estación, con la hora y el día en el que tuvieron alguna base reservada, junto a su cantidad:\n')

# In[43]:


rdd_reservations_date = rdd.filter(lambda x: x[1]['reservations_count']>0).map(lambda x: (x[1]['name'],[x[0],(x[1]['reservations_count'])])).groupByKey().mapValues(tuple)


# In[44]:


p(rdd_reservations_date.take(5))


print('\n 6. ¿Se ha llenado alguna estación completamente? ¿Cuáles y cuantas? \n')

# In[45]:


p(rdd.take(1))


# In[46]:


rdd_full = rdd.filter(lambda x: x[1]['dock_bikes']== x[1]['total_bases']).map(lambda x: x[1]['name']).distinct()


# In[47]:


p(rdd_full.take(20))


# In[48]:


p(rdd_full.count())


print('\n Sólo 123 de las 264 han estado llenas en algún momento. Vamos a ver en mayor profundidad cada una: \n')

# In[49]:


rdd_full_date = rdd.filter(lambda x: x[1]['dock_bikes']== x[1]['total_bases']).map(lambda x: (x[1]['name'],[x[0]])).groupByKey().mapValues(tuple)


# In[50]:


p(rdd_full_date.take(20))


# In[51]:


rdd_full_all = rdd.filter(lambda x: x[1]['dock_bikes']== x[1]['total_bases']).map(lambda x: (x[1]['name'],[x[0]])).countByKey()

p(rdd_full_all)


print('\n De esta forma, vemos qué estaciones han estado más veces llenas. Así, se podría considerar el utilizar algunas de esas bicicletas en otras estaciones, e incluso en un caso extremo, retirar dichas estaciones si no se considera que compense mantenerlas. \n')

print('\n 7. Consideremos una base muy utilizada aquella que ha tenido más del 80% de sus huecos vacíos en algún momento. ¿Cuáles son? ¿Cuándo se han llenado completamente? \n')

# In[52]:


p(rdd.take(1))


# In[53]:


rdd_used = rdd.filter(lambda x: x[1]['free_bases'] >= x[1]['total_bases']*(4/5)).map(lambda x: x[1]['name']).distinct()


# In[54]:


p(rdd_used.take(20))


# In[55]:


p(rdd_used.count())


print('\n La gran mayoría están muy utilizadas en algún momento del día. Veamos cuándo ocurre esto. \n')

# In[56]:


rdd_used_date = rdd.filter(lambda x:  x[1]['free_bases'] >= x[1]['total_bases']*(0.8)).map(lambda x: (x[1]['name'],[x[0],(x[1]['free_bases']/x[1]['total_bases'])])).groupByKey().mapValues(tuple)


print('\n Recogemos los datos de cada estación, con la hora y el día en el que tuvieron una utilización mayor al 80%, junto a su cantidad: \n')

# In[57]:


p(rdd_used_date.take(20))


print('\n 8. Histograma de la cantidad de bicicletas que se han aparcado por estación. \n')

print('\n Reorganizamos rdd para hacer mejor el histograma. \n')

# In[58]:


p(rdd.take(1))


# In[59]:


rdd_duck_hist = rdd.map(lambda x: (x[1]['dock_bikes'], x[0]))


# In[60]:


p(rdd_duck_hist.take(5))


# In[61]:


rdd_duck_hist_c=rdd_duck_hist.countByKey()

p(rdd_duck_hist_c)


# In[62]:


plt.bar(x=rdd_duck_hist_c.keys(), height=rdd_duck_hist_c.values())
plt.show()


print('\n Este histograma puede ser muy interesante, ya que nos ayuda a ver cómo la mayoria de estaciones tienen pocas bicis disponibles la mayoría del tiempo (0-10), y prácticamente nunca están llenas.')

print('\n Ya hemos contestado a nuestras 8 preguntas objetivo sobre el dataset 202106.json obteniendo resultados muy interesantes y que pueden ser verdaderamente útiles en el desarrollo, optimización y expansión de la empresa.')
# 
print('\n En el siguiente punto estudiaremos el dataset 2021_movements.json \n')

print('\n Dataset 202106_movements.json')

print('\n Ahora, vamos a plantear distintas consultas y analizar la base de datos "202106_movements.json"')

print('\n Antes de nada, consideramos conveniente ver, como en el caso anterior, cómo es. \n')


# In[65]:


df2 = spark.read.json('202106_movements.json')


# In[66]:


df2.printSchema()
df2.show()
df2.count()


print('\n Limpiémoslo un poco para que entre bien en la página y podamos visualizar con mayor claridad los resultados. \n')

# In[67]:


df2 = df2.drop('_id','ageRange','zip_code', 'idplug_base', 'idunplug_base')
df2.show()


print('\n Objetivo \n')

print('\n Habiendo visto con claridad la forma de las observaciones de esta base de datos, nos parece interesante responder las siguientes preguntas:')
print('\n 1. ¿Qué cantidad de usuarios hay por cada uno de los tipos?')
print('\n 2. ¿Qué viajes se hicieron el día 19 de junio de 2021? ¿Cuántos fueron? ¿Cuántos se hicieron por cada tipo de usuario?')
print('\n 3. ¿Cuántos viajes se han hecho entre las 15:00 y las 18:00? ¿Cuáles fueron y cuanto duraron? De esos, ¿cuántos duraron entre 20 y 30 minutos? ¿Y entre 10 y 15?')
print('\n 4. ¿Cuántos viajes se han realizado de más de 30 minutos? ¿Cuánto han durado de media?')
print('\n 5. Histograma de viajes según su duración.')
print('\n 6. ¿Cuál es la estación con menos salidas y cuántas son?  ¿Cómo son los viajes desde esa estación?')
print('\n 7. ¿Cuántos viajes se han realizado por día? Gráfico de barras.')
print('\n 8. ¿Qué personas han hecho viajes de ida y vuelta? ¿Cuántos han hecho? ¿Cuál ha sido la duración media de éstos?')

print('\n 0. Organización de los RDD \n')
print('\n Vamos a crear las funciones necesarias para leer bien los RDD y poder hacer las consultas propuestas. \n')

# In[68]:


rdd_base_2 = sc.textFile('202106_movements.json')


# In[69]:


def mapper(line):
    data = json.loads(line)
    u_t = data['user_type']
    u_c = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    time = data['travel_time']
    date = data['unplug_hourTime']
    return u_t, u_c, start, end, time, date


# In[70]:


rdd = rdd_base_2.map(mapper)


# In[71]:


p(rdd.take(3))


print('\n Ahora que lo tenemos limpio y organizado como queríamos, podemos ponernos a responder las preguntas.')

print('\n 1. ¿Qué cantidad de usuarios hay por cada uno de los tipos?')
print('\n En el ejemplo que nos ha subido vemos que esto se puede hacer directamente desde los RDD de la siguiente manera. \n')

# In[72]:


p(rdd.countByKey())


print('\n Si queremos seleccionar los usuarios por tipo y contarlos por separado, se haría así. \n')

# In[73]:


selected_type = 1
rdd_users = rdd.filter(lambda x: x[0]==selected_type).map(lambda x: (x[1],tuple(x[2:])))


print('\n Con el anterior RDD podemos coger un usuario perteneciente al tipo 1 y contar cuántos usuarios pertenecen a ese determinado tipo. \n')

# In[74]:


p(rdd_users.take(1))


# In[75]:


p(rdd_users.count())


print('\n Podemos hacerlo también para los usuarios del tipo 2. \n')

# In[76]:


selected_type = 2
p(rdd_users.count())


print('\n Y para los del tipo 3. \n')

# In[77]:


selected_type = 3
p(rdd_users.count())


print('\n Y para los del tipo 6 y 7 también. \n')

# In[78]:


selected_type = 6
p(rdd_users.count())


# In[79]:


selected_type = 7
p(rdd_users.count())


print('\n Pero esto ya estaba todo hecho en la práctica de ejemplo, hagámoslo con el dataframe que hemos creado para ver cómo funcionaría.')
# 
print('\n Comencemos contando cada tipo, como antes. \n')

# In[80]:


df2.groupBy('user_type').count().show()


print('\n Veamos a ver cómo se haría si queremos seleccionar sólo los del tipo 1. \n')

# In[81]:


df2_1 = df2.filter(df2["user_type"]==1)
df2_1.show()
p(df2_1.count())


print('\n Con el resto de tipos se haría exactamente igual, sólo que cambiando el tipo. \n')

# In[82]:


p(df2.filter(df2["user_type"]==2).count())


# In[83]:


p(df2.filter(df2["user_type"]==3).count())


# In[84]:


p(df2.filter(df2["user_type"]==6).count())


# In[85]:


p(df2.filter(df2["user_type"]==7).count())


print('\n 2. ¿Qué viajes se hicieron el día 19 de junio de 2021? ¿Cuántos fueron? ¿Cuántos se hicieron por cada tipo de usuario?')
print('\n Para responder estas preguntas, el formato en el que nos viene la fecha dada no nos sirve, pues también viene la hora del viaje y nos dificultaría el conteo. Debemos crear un nuevo mapeo. \n')

# In[86]:


def date_mapper(line):
    data = json.loads(line)
    u_t = data['user_type']
    u_c = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    time = data['travel_time']
    date = datetime.fromisoformat(data['unplug_hourTime'][:-1]).date().isoformat()
    return  date, u_c, u_t, start, end, time


# In[87]:


rdd_date = rdd_base_2.map(date_mapper)


# In[88]:


p(rdd_date.take(3))


print('\n Como hemos colocado la fecha en primer lugar, podríamos hacer un countByKey para ver exactamente cuántos viajes se hicieron cada día. \n')

# In[89]:


p(rdd_date.countByKey())


print('\n Como sólo queremos ver cuántos viajes se hicieron el 19 de junio realizamos el siguiente filter. \n')

# In[90]:


selected_date = '2021-06-19'
rdd_dateselected = rdd_date.filter(lambda x: x[0]==selected_date).map(lambda x: (x[1],x[2:]))


print('\n Para ver, como queremos, cuáles fueron los viajes realizados en esa fecha, realizamos un collect. \n')

# In[91]:


p(rdd_dateselected.take(20))


print('\n Y para contarlos, basta hacer el count. Comprobamos que nos sale lo mismo que cuando hicimos el countByKey. \n')

# In[92]:


p(rdd_dateselected.count())


print('\n Nos piden ahora que veamos cuántos de esos viajes se hicieron por tipo de usuario. Deberemos reorganizar la estructura de los datos para poder hacer un countByKey. \n')

# In[93]:


selected_date = '2021-06-19'
rdd_dateselected_byuser = rdd_date.filter(lambda x: x[0]==selected_date).map(lambda x: (x[2],x[1],x[3:]))


# In[94]:


p(rdd_dateselected_byuser.take(1))


print('\n Con el tipo de usuario ya colocado en primer lugar hacemos el countByKey mencionado y obtenemos la cantidad de viajes por tipo de usuario que se hicieron el día 19 de junio de 2021. \n')

# In[95]:


p(rdd_dateselected_byuser.countByKey())

print('\n 3. ¿Cuántos viajes se han hecho entre las 15:00 y las 18:00? ¿Cuáles fueron y cuanto duraron? De esos, ¿cuántos duraron entre 20 y 30 minutos? ¿Y entre 10 y 15?\n')

print('Al igual que con las preguntas anteriores, ahora tenemos el problema de que en los mapeos anteriores aparece siempre la fecha de los viajes, y esto es algo que no nos interesa. Realizaremos un nuevo mapeo que separe la hora del resto de la fecha.\n')

# In[96]:


def time_mapper(line):
    data = json.loads(line)
    u_t = data['user_type']
    u_c = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    time = data['travel_time']
    hour = datetime.fromisoformat(data['unplug_hourTime'][:-1]).time().isoformat()
    return  hour, u_c, u_t, start, end, time


# In[97]:


rdd_time = rdd_base_2.map(time_mapper)


print('\n Podemos, si queremos, ver cuántos viajes se han hecho por hora.\n')

# In[98]:


p(rdd_time.countByKey())


print('\n Como nos piden los viajes entre las 15:00 y las 18:00, debemos sólo tener en cuenta los viajes realizados a las 15, 16 y 17 horas. Realizaremos el siguiente filter.\n')

# In[99]:


times = ['15:00:00', '16:00:00', '17:00:00']
rdd_timeselected = rdd_time.filter(lambda x: x[0] in times).map(lambda x: (x[1],x[2:]))


# In[100]:


p(rdd_timeselected.take(3))


print('\n Como queremos saber cuantos son, hagamos un count.\n')

# In[101]:


p(rdd_timeselected.count())


print('\n Observemos que 25645 + 32095 + 33297 = 91037. Por lo que parece que lo hemos hecho bien.\n')

print('Queremos saber qué viajes fueron y cuánto duraron. Bastaría hacer un collect, pero para que quede más organizada la información, vamos a hacer primero un map para que sólo nos devuelva el id del usuario y el tiempo del viaje.\n')

# In[102]:


rdd_timeselected_duration = rdd_timeselected.map(lambda x: (x[0], x[1][3]))


# In[103]:


p(rdd_timeselected_duration.take(3))


print('\n Para saber cuántos duraron entre 20 y 30 minutos (1200 y 1800 segundos), realizaremos un nuevo filter y un count.\n')

# In[104]:


mintime = 1200
maxtime = 1800
rdd_timeselected_durationselected = rdd_timeselected_duration.filter(lambda x: mintime <= x[1] <= maxtime)


# In[105]:


p(rdd_timeselected_durationselected.take(3))


# In[106]:


p(rdd_timeselected_durationselected.count())


print('\n Observemos que lo bueno que tiene esto es que podemos elegir el intervalo que queramos sólo cambiando el mintime y el maxtime, sin necesidad de cambiar el RDD, pues éste no se evalúa.\n')

print('Por lo que para saber la cantidad de viajes entre 10 y 15 minutos (600 y 900 segundos) basta hacer lo siguiente.\n')

# In[107]:


mintime = 600
maxtime = 900
p(rdd_timeselected_durationselected.count())


print('\n 4. ¿Cuántos viajes se han realizado de más de 30 minutos? ¿Cuánto han durado de media?\n')

print('Volvemos a utilizar el RDD creado con el primer mapeo.\n')

# In[108]:


p(rdd.take(3))


print('\n Hagamos un filter para obtener sólo los viajes de más de 30 minutos (1800 segundos).\n')

# In[109]:


minduration = 1800
rdd_durationfiltered = rdd.filter(lambda x: x[4] >= minduration).map(lambda x: (x[4]))


# In[110]:


p(rdd_durationfiltered.take(3))


print('\n Para saber cuántos viajes de más de 30 min hay, bastará contarlos con count. Lo guardamos en una variable pues la vamos a utilizar después para calcular la media de minutos por viaje.\n')

# In[111]:


total = rdd_durationfiltered.count()
p(total)


print('\n Para calcular la media de minutos por viaje, utilizaremos la función importada al principio add junto con un reduce para obtener la suma total y dividiremos entre el número total de viajes de más de 30 minutos, calculados antes. Lo pasaremos a minutos dividiendo entre 60 para que quede mejor.\n')

# In[112]:


media = (rdd_durationfiltered.reduce(add)/total)/60
p(media)


print('\n 5. Histograma de viajes según su duración. Reorganicemos el RDD original para que sólo contenga la duración de los viajes y el id del usuario.\n')

# In[113]:


rdd_duration = rdd.map(lambda x: (x[4], x[1]))


# In[114]:


p(rdd_duration.take(3))


print('\n De esta manera, basta hacer un countByKey para obtener los datos del histograma que queremos.\n')

# In[115]:


dict_duration = rdd_duration.countByKey()
print(dict_duration)


# Para visualizarlo utilizamos la librería matplotlib.pyplot importada al principio del documento.

# In[116]:


# plt.bar(x=dict_duration.keys(), height=dict_duration.values())

print('\n Observemos que se nos presenta un problema, hay demasiadas clases como para que python pueda dibujarlo correctamente. Por lo que vamos a agrupar los tiempos de viaje en subgrupos de tamaño 100. De forma que los viajes que duren entre 0 y 50 segundos irán al grupo 0, los que duren entre 50 y 150 segundos al del grupo 1, los que duren entre 150 y 250 segundos al del grupo 2 y así con todos. Vemos también que hay tiempos de viaje superiores a 20000 segundos (es, decir, de 5 horas de trayecto) como no tienen mucho sentido los eliminaremos del histograma. Recurriremos a la función round de python para hacer esto. Crearemos otro RDD a partir del anterior con los datos redondeados.\n')

# In[117]:


rdd_duration_rounded = rdd_duration.filter(lambda x: x[0] < 20000).map(lambda x: (round(x[0],-2)/100, x[1]))


# In[118]:


p(rdd_duration_rounded.take(10))


print('\n Ya podemos agruparlos como antes y visualizarlos.\n')

# In[119]:


dict_duration = rdd_duration_rounded.countByKey()
p(dict_duration)


# In[120]:


# plt.bar(x=dict_duration.keys(), height=dict_duration.values())


print('\n Vemos que la gran mayoría de los datos se encuentran entre los grupos 0 y 40. Hagamos un filtro de esos datos y representémoslos para verlo mejor.\n')

# In[121]:


rdd_duration_filtered = rdd_duration_rounded.filter(lambda x: x[0] <= 40)


# In[122]:


dict_duration_filt = rdd_duration_filtered.countByKey()
p(dict_duration_filt)


# In[123]:


plt.bar(x=dict_duration_filt.keys(), height=dict_duration_filt.values())
plt.show()


print('\n Tras filtrar los grupos menores a 40, vemos cómo la mayoría se acumulan entre los grupos 5 y 10. Una buena idea sería realizar ofertas de si se hacen 10 viajes te regalamos el siguiente o alguna otra oferta del estilo que se ajuste a la disposición de viajes por usuario que hemos podido observar gracias a esta consulta.\n')

print('\n 6.  ¿Cuál es la estación con menos salidas y cuántas son?  ¿Cómo son los viajes desde esa estación?\n')
# 
print('Reorganicemos el RDD original para que contenga el id de la estación de salida y el id de usuario.\n')

# In[124]:


rdd_less = rdd.map(lambda x: (x[2], x[1]))


# In[125]:


p(rdd_less.take(2))


# In[126]:


rdd_less_k = rdd_less.countByKey()
p(rdd_less_k)


print('\n Veamos cuál es el mínimo número de salidas.\n')

# In[127]:


p(min(list(rdd_less_k.values())))


print('\n Parece que hay una estación con únicamente 22 salidas. Veamos cúal es.\n')

# In[128]:


p(list(rdd_less_k.values()).index(22))


# In[129]:


p(list(rdd_less_k.keys())[262])


print('\n La estación de salida con menos viajes es la de id 2009, en la que solo se han producido 22 salidas. Vamos a ver ahora cómo son los viajes desde la misma.\n')

# In[130]:


rdd_less_info = rdd.filter(lambda x: x[2]==2009).map(lambda x: (x[1],x[3],x[4]))


# In[131]:


p(rdd_less_info.take(20))


print('\n La mayoría de los viajes desde la estación 2009 acaban regresando a la misma y son de duraciones cortas.\n')

print('\n 7. ¿Cuántos viajes se han realizado por día? Gráfico de barras.\n')

print('Utilizaremos el rdd llamado rdd_date creado con el mapeo de la pregunta 2 para realizar esta consulta.\n')

# In[132]:

rdd_travel_day = rdd_date.countByKey()
p(rdd_travel_day)


print('\n Hagamos el gráfico de barras para así, a simple vista, poder ver qué días se han realizado más viajes.\n')

# In[133]:


plt.bar(x=rdd_travel_day.keys(), height=rdd_travel_day.values())
plt.xticks(rotation=90)
plt.show()



print('\n Con esto, podemos ver cómo el número de viajes varía mucho según el día, abarcando un rango de unos 7800 viajes diaros hasta los 18000. Lo que sí se puede observar de forma más o menos clara es que se repite el patrón de haber menos viajes en fin de semana que entre semana.\n')

print('\n 8. ¿Cuántas personas han hecho viajes de ida y vuelta? ¿En qué estaciones ha ocurrido más a menudo? ¿Cuál ha sido la duración media de éstos?\n')

# In[134]:


p(rdd.take(1))


# In[135]:


rdd_round_trip = rdd.filter(lambda x: x[2]==x[3]).map(lambda x: (x[2],x[1],x[3],x[4]))


# In[136]:


p(rdd_round_trip.take(20))


# In[137]:


print('\n', rdd_round_trip.count(), 'personas han hecho viajes de ida y vuelta \n')


# In[138]:


p(rdd_round_trip.countByKey())


print('\n Con esto vemos que hay varias estaciones en las que es común que se hagan a veces viajes de ida y vuelta, produciendose más de 200 veces en algunas.\n')
# 
print('Calculemos y mostremos el tiempo medio de los viajes de ida y vuelta de cada estación.\n')

# In[139]:


rdd_round_mean = rdd.filter(lambda x: x[2]==x[3]).map(lambda x: (x[2],x[4])).groupByKey().mapValues(lambda x: (sum(x)/len(x)))


# In[140]:


p(rdd_round_mean.take(20))


print('\n Conclusiones\n')

print('\n Tras haber resuelto preguntas de interés a partir de las dos bases de datos (previamente analizandolas y realizando pequeños cambios que nos ayudarían a trabajar con ellas), hemos sido capaces de ver la gran utilidad y valor que tiene esta asignatura para ayudar al rendimiento óptimo de empresas, en este caso, Bicimad print \n')

print('El trabajo ha sido muy satisfactorio puesto que hemos sido capaces de resolver cada una de las preguntas objetivo, es decir, un total de 16 preguntas, algunas con mayor dificultad, pero lo hemos conseguido \n')
print('En cuanto a los resultados obtenidos, se han ido comentando a lo largo de este notebook y se han ido planteando posibles preguntas extras. Por ejemplo, otra a resolver podría ser: ¿Desde qué tres estaciones se inician más viajes? o ¿En cuales tres se concluyen más trayectos? que podría dejarse como futura continuación.\n')
