from pyspark.sql import SparkSession, functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Iniciar Spark
spark = SparkSession.builder.appName(
    "RendimientoAcademico"
).master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Cargar datos desde HDFS
ruta = "hdfs://localhost:9000/Tarea3_RendimientoAcademico/student-por.csv"
df = spark.read.option("header", True).option(
    "sep", ";"
).option("inferSchema", True).csv(ruta)

print("Registros originales:", df.count())
print("Duplicados completos:",
      df.count() - df.dropDuplicates().count())

# Revisar valores faltantes
columnas = ["age", "studytime", "absences", "G1", "G2", "G3"]
df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in columnas
]).show()

# Preparar los datos sin eliminar estudiantes
# que coinciden solo en algunas variables
datos = df.dropDuplicates().select(*columnas).dropna().filter(
    "G1 BETWEEN 0 AND 20 AND G2 BETWEEN 0 AND 20 AND G3 BETWEEN 0 AND 20"
)
print("Registros limpios:", datos.count())

# Promedios académicos
promedios = datos.agg(*[
    F.avg(c).alias(c) for c in ["G1", "G2", "G3"]
]).first()
valores = [promedios[c] for c in ["G1", "G2", "G3"]]
print("Promedios G1, G2 y G3:",
      [round(v, 2) for v in valores])

# Análisis del tiempo de estudio
estudio = datos.groupBy("studytime").agg(
    F.count("*").alias("Estudiantes"),
    F.round(F.avg("G3"), 2).alias("Promedio_Final")
).orderBy("studytime")
estudio.show()

# Análisis de las ausencias
ausencias = datos.withColumn(
    "Grupo_Ausencias",
    F.when(F.col("absences") == 0, "0 ausencias")
     .when(F.col("absences") <= 5, "1 a 5 ausencias")
     .otherwise("6 o mas ausencias")
).groupBy("Grupo_Ausencias").agg(
    F.count("*").alias("Estudiantes"),
    F.round(F.avg("G3"), 2).alias("Promedio_Final")
)
ausencias.show(truncate=False)

# Gráfica 1: calificaciones
plt.figure(figsize=(7, 4))
plt.bar(["G1", "G2", "G3"], valores)
plt.title("Promedio de calificaciones")
plt.ylabel("Calificacion (0 a 20)")
plt.ylim(0, 20)
plt.tight_layout()
plt.savefig("grafica_promedios.png", dpi=150)
plt.close()

# Gráfica 2: tiempo de estudio
filas_estudio = estudio.collect()
plt.figure(figsize=(7, 4))
plt.bar(
    [str(r["studytime"]) for r in filas_estudio],
    [r["Promedio_Final"] for r in filas_estudio]
)
plt.title("Rendimiento segun tiempo de estudio")
plt.xlabel("Categoria de tiempo de estudio")
plt.ylabel("Promedio final")
plt.ylim(0, 20)
plt.tight_layout()
plt.savefig("grafica_estudio.png", dpi=150)
plt.close()

# Gráfica 3: ausencias
filas_ausencias = sorted(
    ausencias.collect(),
    key=lambda r: {
        "0 ausencias": 0,
        "1 a 5 ausencias": 1,
        "6 o mas ausencias": 2
    }[r["Grupo_Ausencias"]]
)
plt.figure(figsize=(8, 4))
plt.bar(
    [r["Grupo_Ausencias"] for r in filas_ausencias],
    [r["Promedio_Final"] for r in filas_ausencias]
)
plt.title("Rendimiento segun ausencias")
plt.ylabel("Promedio final")
plt.ylim(0, 20)
plt.tight_layout()
plt.savefig("grafica_ausencias.png", dpi=150)
plt.close()
print("Graficas generadas correctamente")
spark.stop()# Gráfica 3: ausencias
filas_ausencias = sorted(
    ausencias.collect(),
    key=lambda r: {
        "0 ausencias": 0,
        "1 a 5 ausencias": 1,
        "6 o mas ausencias": 2
    }[r["Grupo_Ausencias"]]
)
plt.figure(figsize=(8, 4))
plt.bar(
    [r["Grupo_Ausencias"] for r in filas_ausencias],
    [r["Promedio_Final"] for r in filas_ausencias]
)
plt.title("Rendimiento segun ausencias")
plt.ylabel("Promedio final")
plt.ylim(0, 20)
plt.tight_layout()
plt.savefig("grafica_ausencias.png", dpi=150)
plt.close()

print("Graficas generadas correctamente")
# Analisis mediante Spark SQL
datos.createOrReplaceTempView("estudiantes")
print("RESULTADOS SPARK SQL")
spark.sql("""
SELECT studytime AS Tiempo_Estudio,
       COUNT(*) AS Estudiantes,
       ROUND(AVG(G3), 2) AS Promedio_Final
FROM estudiantes
GROUP BY studytime
ORDER BY studytime
""").show()

spark.stop()
