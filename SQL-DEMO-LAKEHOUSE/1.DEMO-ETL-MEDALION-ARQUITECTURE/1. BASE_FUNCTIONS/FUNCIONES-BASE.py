# Databricks notebook source

# client_id = dbutils.secrets.get(scope="scope-keyvault", key="clientid-serviceprincipal-adls-databricks")
# client_secret = dbutils.secrets.get(scope="scope-keyvault", key="clientsecret-serviceprincipal-adls-databricks")
# tenant_id = dbutils.secrets.get(scope="scope-keyvault", key="tenantid-serviceprincipal-adls-databricks")

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.saninjadatafsd01.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.saninjadatafsd01.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.saninjadatafsd01.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.saninjadatafsd01.dfs.core.windows.net", client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.saninjadatafsd01.dfs.core.windows.net", f"https://login.microsoftonline.com/c3840b57-38c0-4bb2-a938-7d27557dd7b2/oauth2/token")

# COMMAND ----------

def crear_carpeta(container, storage_account_name, path):
    print(f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}")
    dbutils.fs.mkdirs(f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}")

# COMMAND ----------

def escribir_archivo(container, storage_account_name, path, file_name, contenido):
    print(f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}/{file_name}")
    dbutils.fs.put(f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}/{file_name}", contenido, overwrite=True)

# COMMAND ----------


def copiar_archivos_adls(origen, destino):
    print(f"origen : " + origen)
    print(f"origen : " + destino)
    dbutils.fs.cp(origen, destino)


# COMMAND ----------

def listar_contenido_carpeta(container, storage_account_name, path):
    print(f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}")
    display(dbutils.fs.ls(f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}"))

# COMMAND ----------

# %fs ls abfss://datalake@saninjadatafsd01.dfs.core.windows.net/

# COMMAND ----------


# # Crear una carpeta en ADLS
# %fs mkdirs abfss://<container>@saninjadatafsd01.dfs.core.windows.net/<path>

# # Escribir un archivo en la carpeta creada
# %fs put abfss://<container>@saninjadatafsd01.dfs.core.windows.net/<path>/archivo.txt "Contenido del archivo"

# # Listar contenido de la carpeta
# %fs ls abfss://<container>@saninjadatafsd01.dfs.core.windows.net/<path>

# COMMAND ----------

def read_file_from_blob(storage_account_name, container_name, file_path):
    full_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"
    return spark.read.format("csv").option("header", "true").load(full_path)

# COMMAND ----------

def write_data_to_container(df, storage_account_name, container_name, file_path, file_format="parquet", mode="overwrite"):
    full_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"
    df.write.format(file_format).mode(mode).save(full_path)
