# Análisis del rendimiento académico de estudiantes con Apache Spark

## Descripción del proyecto

Proyecto desarrollado para la Tarea 3 del curso Big Data de la UNAD, orientado al análisis del rendimiento académico de estudiantes mediante procesamiento por lotes (*batch*) con Apache Spark y PySpark.

Se utiliza el conjunto de datos *Student Performance* del repositorio UCI Machine Learning Repository, específicamente el archivo `student-por.csv`, que contiene 649 registros.

## Objetivo

Analizar las calificaciones académicas y su relación descriptiva con el tiempo semanal dedicado al estudio y la cantidad de ausencias.

## Tecnologías utilizadas

- Ubuntu 22.04
- Apache Hadoop 3.3.6
- Apache Spark 3.5.8
- Python y PySpark
- Matplotlib
- Hadoop HDFS

## Fuente de datos

UCI Machine Learning Repository:

https://archive.ics.uci.edu/dataset/320/student+performance

Archivo utilizado: `student-por.csv`.

Variables: `age`, `studytime`, `absences`, `G1`, `G2` y `G3`.

## Procesamiento realizado

El programa `analisis_rendimiento.py` permite:

1. Leer el conjunto de datos almacenado en Hadoop HDFS.
2. Verificar registros duplicados y valores faltantes.
3. Limpiar y validar las calificaciones académicas.
4. Calcular los promedios de G1, G2 y G3.
5. Analizar el rendimiento según el tiempo de estudio.
6. Comparar las calificaciones según las ausencias.
7. Generar tres gráficas mediante Matplotlib.

La versión actualizada del programa también incorpora una consulta con Spark SQL.

## Instrucciones de ejecución

### 1. Preparar el entorno

Tener Hadoop HDFS y Apache Spark configurados y funcionando.

Instalar Matplotlib en el entorno de Python:

```bash
python3 -m pip install --user matplotlib
```

### 2. Descargar el conjunto de datos

Obtener el archivo `student-por.csv` desde el repositorio UCI.

### 3. Cargar el archivo en HDFS

Crear el directorio:

```bash
hdfs dfs -mkdir -p /Tarea3_RendimientoAcademico
```

Subir el archivo:

```bash
hdfs dfs -put student-por.csv /Tarea3_RendimientoAcademico/
```

### 4. Ejecutar el análisis

Desde la carpeta que contiene el programa:

```bash
spark-submit --master 'local[1]' analisis_rendimiento.py
```

El programa utiliza la ruta:

`hdfs://localhost:9000/Tarea3_RendimientoAcademico/student-por.csv`

Por lo tanto, Hadoop debe estar disponible en esa dirección.

## Resultados obtenidos

Se procesaron 649 registros y se obtuvieron los siguientes promedios:

- G1: 11,40 puntos.
- G2: 11,57 puntos.
- G3: 11,91 puntos.

El grupo de estudiantes que dedica entre 5 y 10 horas semanales al estudio presentó un promedio final de 13,23 puntos.

Los estudiantes con seis o más ausencias registraron un promedio final de 11,51 puntos.

## Archivos generados

El programa produce las siguientes gráficas:

- `grafica_promedios.png`
- `grafica_estudio.png`
- `grafica_ausencias.png`

## Práctica complementaria de streaming

Como actividad adicional se implementó Apache Kafka 3.7.2 y Spark Structured Streaming 3.5.8 para procesar datos simulados de temperatura y humedad mediante el tópico `sensores`.

Esta práctica utiliza el programa `streaming_sensores.py`, que se incorpora por separado al repositorio.

## Conclusión

El proyecto permitió aplicar herramientas del ecosistema Big Data al procesamiento y análisis descriptivo de información académica, utilizando Hadoop HDFS para el almacenamiento y Apache Spark para la transformación, el cálculo de estadísticas y la generación de visualizaciones.
