# Project summary

## Requeriment
Una startup llamada Sparkify quiere analizar los datos que han estado recopilando sobre las canciones y la actividad de los usuarios en su nueva transmisión de música.
aplicación. El equipo de análisis está particularmente interesado en comprender qué canciones escuchan los usuarios. Actualmente no cuentan con
manera fácil de consultar sus datos.

## Solution
En este proyecto, se definen tablas de hechos y dimensiones para un **esquema en estrella** para un enfoque analítico particular y una canalización ETL.
que transfiere datos de archivos en dos directorios locales a estas tablas. las tablas y los datos están en Postgres y el ETL es
construido usando Python y SQL.

## Purposes of the star schema
* Sparkify tendrá la posibilidad de realizar consultas más sencillas, lo que significa que la lógica de unión para Star Schema es más simple.
   que la lógica de unión de otro modelo altamente normalizado.
* Tiene un rendimiento de consultas eficiente. Star Schemas puede proporcionar una mejora del rendimiento para las aplicaciones de informes de solo lectura que
   otros tipos de esquemas.
* Agregaciones más rápidas, lo que significa que consultas más simples realizadas en un esquema en estrella pueden producir un rendimiento mejorado para todos
   operaciones de agregación.
  
  
# Project files

1. **test.ipynb** muestra las primeras filas de cada tabla y cuenta los registros insertados en cada tabla.
2. **create_tables.py** suelta y crea las tablas.
3. **etl.ipynb** lee y procesa un único archivo de song_data y log_data y carga los datos en las tablas.
    Este cuaderno contiene instrucciones detalladas sobre el proceso ETL para cada una de las tablas.
4. **etl.py** lee y procesa archivos de song_data y log_data y los carga en las tablas.
5. **sql_queries.py** contiene todas las consultas SQL y se importa a los últimos tres archivos anteriores.


# How does it work

### step 1: Ejecute %run create_tables.py en una terminal para crear todas las tablas

```bash
python create_tables.py
```

### step 2: %run etl.py en una terminal para cargar los datos de los archivos a las tablas

```bash
python etl.py
```

### step 3: Ejecute las consultas en el cuaderno test.ipynb para verificar si los datos se cargaron

1. Abra el cuaderno ***test.ipynb***
2. Cargue el módulo sql: ***%load_ext sql***
3. Conéctese a la base de datos: ***%sql postgresql://anthonylinux:password@127.0.0.1/sparkifydb***
4. Ejecute las consultas

Estas son las consultas que podrás encontrar en la libreta:

* %sql SELECT * FROM songplays LIMIT 5
* %sql SELECT * FROM users LIMIT 5
* %sql SELECT * FROM songs LIMIT 5
* %sql SELECT * FROM artists LIMIT 5
* %sql SELECT * FROM time LIMIT 5

* %sql SELECT COUNT(*) FROM users
* %sql SELECT COUNT(*) FROM artists
* %sql SELECT COUNT(*) FROM songs
* %sql SELECT COUNT(*) FROM time
* %sql SELECT COUNT(*) FROM songplays


