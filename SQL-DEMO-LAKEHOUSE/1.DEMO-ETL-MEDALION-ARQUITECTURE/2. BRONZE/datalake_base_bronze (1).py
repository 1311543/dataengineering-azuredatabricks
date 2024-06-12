# Databricks notebook source
# MAGIC %run /Workspace/Users/licibeth.delacruz_ninjadata.academy#ext#@licibethdelacruzninjadata.onmicrosoft.com/WORKSPACE/FUNCIONES-BASE

# COMMAND ----------

file_path = "bronze/landing"
crear_carpeta(container_name, storage_account_name, file_path)

# COMMAND ----------

storage_account_name = "saninjadatafsd01"
container_name = "datalake"
file_path = "bronze/Persona"
crear_carpeta(container_name, storage_account_name, file_path)

# COMMAND ----------

file_path = "bronze/Transacciones"
crear_carpeta(container_name, storage_account_name, file_path)

# COMMAND ----------

dbutils.fs.cp("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/bronze/landing/persona.data", "abfss://datalake@saninjadatafsd01.dfs.core.windows.net/bronze/Persona/persona.data", recurse=True)

# COMMAND ----------

files = dbutils.fs.ls("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/bronze/landing")

# COMMAND ----------


for i in files:
    # print(i)
    if i.path.startswith("abfss://datalake@saninjadatafsd01.dfs.core.windows.net/bronze/landing/transacciones"):
        print(i.path)
        base_file = i.path[70:]
        print(base_file)
        dbutils.fs.cp(i.path, f"abfss://datalake@saninjadatafsd01.dfs.core.windows.net/bronze/Transacciones/{base_file}")

# COMMAND ----------

dbutils.fs.help()
