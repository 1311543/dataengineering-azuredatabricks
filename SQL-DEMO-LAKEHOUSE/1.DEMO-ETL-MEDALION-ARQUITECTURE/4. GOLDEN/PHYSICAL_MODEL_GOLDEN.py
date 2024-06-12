# Databricks notebook source
# MAGIC %run /Workspace/Users/licibeth.delacruz_ninjadata.academy#ext#@licibethdelacruzninjadata.onmicrosoft.com/WORKSPACE/FUNCIONES-BASE

# COMMAND ----------

storage_account_name = "saninjadatafsd01"
container_name = "datalake"
path_transaccionByPerson = "golden/transaccionByPerson"
crear_carpeta(container_name, storage_account_name, path_transaccionByPerson)

# COMMAND ----------

persona = spark.sql("select * from ninjadatadbrworkspace01.ninjadatabase.persona").alias("table1")

# COMMAND ----------

transacciones = spark.sql("select * from ninjadatadbrworkspace01.ninjadatabase.transacciones").alias("table2")

# COMMAND ----------

joinned_persona_transaccion = persona.join(transacciones, persona.ID_PERSONA == transacciones.ID_PERSONA, "inner")

# COMMAND ----------

from pyspark.sql.functions import col
joinned_persona_transaccion = joinned_persona_transaccion.select(col("table1.ID_PERSONA"),
col("table1.NOMBRE_PERSONA"),
col("table1.EDAD").alias("EDAD_PERSONA"),
col("table1.ID_EMPRESA"),
col("table2.MONTO"),
col("table2.FECHA")
)


# COMMAND ----------

filtered_transactions = joinned_persona_transaccion.filter(joinned_persona_transaccion.EDAD_PERSONA > 25)

# COMMAND ----------

from pyspark.sql.functions import substring, trim
final_dataframe = filtered_transactions.filter(substring(trim(filtered_transactions.NOMBRE_PERSONA), 0, 1) == 'A')

# COMMAND ----------

spark.sql("""
        CREATE TABLE IF NOT EXISTS ninjadatadbrworkspace01.ninjadatabase.transaccionByPerson(
        ID_PERSONA STRING,
        NOMBRE_PERSONA STRING,
        EDAD_PERSONA INTEGER,
        ID_EMPRESA STRING,
        MONTO DOUBLE,
        FECHA DATE
        )
        USING DELTA
        PARTITIONED BY (ID_EMPRESA, FECHA)
        LOCATION 'abfss://datalake@saninjadatafsd01.dfs.core.windows.net/golden/transaccionByPerson'
        COMMENT 'Tabla de transacciones'
    """)

# COMMAND ----------

final_dataframe.write.mode("overwrite").format("delta").partitionBy("ID_EMPRESA", "FECHA").save("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/golden/transaccionByPerson")

# COMMAND ----------

spark.sql("select * from ninjadatadbrworkspace01.ninjadatabase.transaccionByPerson").display()
