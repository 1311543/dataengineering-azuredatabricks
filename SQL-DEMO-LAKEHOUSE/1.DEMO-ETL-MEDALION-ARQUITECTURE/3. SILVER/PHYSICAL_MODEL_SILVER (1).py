# Databricks notebook source
# MAGIC %run /Workspace/Users/licibeth.delacruz_ninjadata.academy#ext#@licibethdelacruzninjadata.onmicrosoft.com/WORKSPACE/FUNCIONES-BASE

# COMMAND ----------

storage_account_name = "saninjadatafsd01"
container_name = "datalake"
path_persona = "silver/Persona"
crear_carpeta(container_name, storage_account_name, path_persona)

# COMMAND ----------

path_transacciones = "silver/Transacciones"
crear_carpeta(container_name, storage_account_name, path_transacciones)

# COMMAND ----------

df_persona = spark.read.option("header", "true").option("delimiter", "|").format("csv").load("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/bronze/Persona/persona.data")

# COMMAND ----------

# MAGIC %md
# MAGIC ![Insert Image](https://miro.medium.com/v2/resize:fit:778/1*ub8P2UqVbHYpHpepR8822Q.png)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ninjadatadbrworkspace01.ninjadatabase.persona
    (
        ID_PERSONA STRING,
        NOMBRE_PERSONA STRING,
        TELEFONO STRING,
        CORREO STRING,
        FECHA_INGRESO DATE,
        EDAD INT,
        SALARIO DOUBLE,
        ID_EMPRESA STRING
    )
    USING DELTA
    PARTITIONED BY (ID_EMPRESA)
    LOCATION 'abfss://datalake@saninjadatafsd01.dfs.core.windows.net/silver/Persona'
    COMMENT 'Tabla de información de personas'
""")

# COMMAND ----------

spark.sql("DROP TABLE ninjadatadbrworkspace01.ninjadatabase.persona")

# COMMAND ----------

dbutils.fs.rm("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/silver/Persona/", recurse=True)

# COMMAND ----------

spark.sql("select * from ninjadatadbrworkspace01.ninjadatabase.persona")

# COMMAND ----------

from pyspark.sql.types import StringType, DateType, IntegerType,DoubleType
from pyspark.sql.functions import col
df_persona = df_persona.select(
    col("ID").cast(StringType()).alias("ID_PERSONA"),
    col("NOMBRE").cast(StringType()).alias("NOMBRE_PERSONA"),
    col("TELEFONO").cast(StringType()).alias("TELEFONO"),
    col("CORREO").cast(StringType()).alias("CORREO"),
    col("FECHA_INGRESO").cast(DateType()).alias("FECHA_INGRESO"),
    col("EDAD").cast(IntegerType()).alias("EDAD"),
    col("SALARIO").cast(DoubleType()).alias("SALARIO"),
    col("ID_EMPRESA").cast(StringType()).alias("ID_EMPRESA"),
    )

# COMMAND ----------

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DateType, IntegerType,DoubleType
schema_persona = StructType([
    StructField("ID_PERSONA", StringType()),
    StructField("NOMBRE_PERSONA", StringType()),
    StructField("TELEFONO", StringType()),
    StructField("CORREO", StringType()),
    StructField("FECHA_INGRESO", DateType()),
    StructField("EDAD", IntegerType()),
    StructField("SALARIO", DoubleType()),
    StructField("ID_EMPRESA", StringType())
])

# COMMAND ----------

casted_df_persona = spark.read.option("delimiter", "|").option("header", "true").csv("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/bronze/Persona/persona.data", schema=schema_persona)

# COMMAND ----------

casted_df_persona.display()

# COMMAND ----------

df_persona.printSchema()

# COMMAND ----------

df_persona.write.mode("overwrite").format("delta").partitionBy("ID_EMPRESA").save("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/silver/Persona")

# COMMAND ----------

# MAGIC %md
# MAGIC Transacciones

# COMMAND ----------

df_transacciones = spark.read.format("csv").option("header", "true").load("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/bronze/Transacciones/*")

# COMMAND ----------

display(df_transacciones)

# COMMAND ----------

spark.sql("""
        CREATE TABLE IF NOT EXISTS ninjadatadbrworkspace01.ninjadatabase.transacciones(
        ID_PERSONA STRING,
        ID_EMPRESA STRING,
        MONTO DOUBLE,
        FECHA DATE
        )
        USING DELTA
        PARTITIONED BY (FECHA)
        LOCATION 'abfss://datalake@saninjadatafsd01.dfs.core.windows.net/silver/Transacciones'
        COMMENT 'Tabla de transacciones'
    """)

# COMMAND ----------

spark.sql("DROP TABLE ninjadatadbrworkspace01.ninjadatabase.transacciones")
dbutils.fs.rm("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/silver/Transacciones/", recurse=True)

# COMMAND ----------

df_transacciones = df_transacciones.select(col("ID_PERSONA").cast(StringType()).alias("ID_PERSONA"),
                        col("ID_EMPRESA").cast(StringType()).alias("ID_EMPRESA"),
                        col("MONTO").cast(DoubleType()).alias("MONTO"),
                        col("FECHA").cast(DateType()).alias("FECHA")
                        )

# COMMAND ----------

df_transacciones.write.mode("overwrite").format("delta").partitionBy("FECHA").save("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/silver/Transacciones")
