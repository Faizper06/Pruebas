# Databricks notebook source
# MAGIC %md
# MAGIC <style>
# MAGIC .mark-box {
# MAGIC   background-color: #f3f3f3;
# MAGIC   border-left: 6px solid #4CAF50;
# MAGIC   padding: 12px 24px;
# MAGIC   font-family: monospace;
# MAGIC   font-size: 14px;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="mark-box">
# MAGIC <b>🏢 Empresa:</b> BICODE SAS<br>
# MAGIC <b>👨‍💻 Autor:</b> Ingeniero Fabian Izquierdo Perez<br>
# MAGIC <b>📅 Fecha:</b> 23/06/2025<br>
# MAGIC <b>🛠️ Tipo:</b> Data Processing<br>
# MAGIC <b>✉️ Email:</b> developer@bicode.co<br>
# MAGIC <b>📌 Descripción:</b><br>
# MAGIC Este notebook documenta la creación de <b>puntos de montaje seguros</b> en Azure Databricks sobre ADLS Gen2.<br><br>
# MAGIC Se aplican <b>buenas prácticas de seguridad</b> mediante el uso de:
# MAGIC <ul>
# MAGIC   <li>App Registrations con permisos mínimos necesarios</li>
# MAGIC   <li>Azure Key Vault para gestión segura de secretos</li>
# MAGIC   <li>Mounts configurados con scopes seguros</li>
# MAGIC </ul>
# MAGIC Esto permite establecer una arquitectura robusta, centralizada y segura para el acceso a los datos en el lakehouse.
# MAGIC
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration - Mount Points

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. _For this configuration the following services must be provisioned:_

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - 1. #### _Creacion de un servicio en azure Key vault:_
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="https://learn.microsoft.com/es-es/dotnet/azure/media/azure-key-vault.svg" alt="Key Vault" width="100"/>
# MAGIC </div>
# MAGIC - 2. #### _Creacion de un Azure storage Account:_
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="https://learn.microsoft.com/es-es/dotnet/azure/media/storage-blobs.svg" alt="Key Vault" width="100"/>
# MAGIC </div>
# MAGIC - 3. #### _Creacion de un Azure Databricks:_ 
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="https://www.databricks.com/sites/default/files/2023-03/azure-feature-1.jpg?v=1724844237" alt="Key Vault" width="100"/>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. _Initial Configuration:_

# COMMAND ----------

# MAGIC %md
# MAGIC - 1. Crear un Scope En Azure Databricks
# MAGIC > Una Opción es creandolo desde el servicio de databricks, utilizando "#secrets/createScope".  
# MAGIC > Ejemplo : https://adb-****************.10.azuredatabricks.net/#secrets/createScope  
# MAGIC > Este Scope creado debe ir configurado con el DNS Name del key vault donde se van a almacenar los secretos
# MAGIC
# MAGIC - 2. Creacion de app registration.  
# MAGIC > https://portal.azure.com/#view/Microsoft_AAD_IAM/ActiveDirectoryMenuBlade/~/RegisteredApps  
# MAGIC
# MAGIC - 3. Rol en cuenta de almacenamiento al app registration.  
# MAGIC > Rol = "Storage Blob Data Contributor"
# MAGIC
# MAGIC - 4. Rol Sobre el Key Vault creado a la entidad administrada "AzureDatabricks".  
# MAGIC > Rol = "Key Vault Secrets User"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. _Creation of secrets:_

# COMMAND ----------

# MAGIC %md
# MAGIC En el Azure Key Vaul creado se deben crear los siguientes secretos:  
# MAGIC - 1. _ClienteId_ : Este codigo se obtiene desde el App Registration - Application (client) ID
# MAGIC - 2. _ClientSercret_ : Este codigo se obtiene desde Certificados y secretos de App Registration, Campo  "valor"
# MAGIC - 3. _TenantId_: Este codigo se obtiene desde el Key vault, variable Directory ID  
# MAGIC - 4. _sase_xxxxx_: Nombre del storage account
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. _Creating the mount point:_  
# MAGIC A continuacion se debe implementar el siguiente codigo:

# COMMAND ----------

# DBTITLE 1,Configuration
# Relacion de los diferentes contenedores de montaje #
MOUNT_CONTAINER_OPERATION = "operacion"
MOUNT_CONTAINER_LANDING = "bronze"
MOUNT_CONTAINER_TRUSTED = 'silver'
MOUNT_CONTAINER_REFINED = 'gold'
MOUNT_CONTAINER_CATALOGING = 'maestro-negocio'
# Variables de los recretos almacenados en el Key Vault #
TENANT_KEY = "TenantId"
CLIENT_KEY = "ClienteId"
SECRET_KEY = "ClientSercret"
# Nombre del Scope creardo en databricks
ENVIRONMENT_SCOPE = 'StorageScope'
LAKE_ACCOUNT = dbutils.secrets.get(ENVIRONMENT_SCOPE, 'sase_xxxxx')
ALIAS_MOUNT = "XXXXX" #Nombre que se asigna para identificar el mount

configs = {"fs.azure.account.auth.type": "OAuth",
             "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
             "fs.azure.account.oauth2.client.id": "id",
             "fs.azure.account.oauth2.client.secret": "secret",
             "fs.azure.account.oauth2.client.endpoint": "endpoint"}

# COMMAND ----------

# DBTITLE 1,Def funtion
# Method to create mount point for all the containers in Datalake
def mount_point(ALIAS_MOUNT,str_mount_point, str_datalake_point, str_key_client, str_key_secret, str_tenant_secret, str_Enviroment_Scope, configs):  
  
  configs["fs.azure.account.oauth2.client.id"] = dbutils.secrets.get(str_Enviroment_Scope, str_key_client)
  configs["fs.azure.account.oauth2.client.secret"] = dbutils.secrets.get(str_Enviroment_Scope, str_key_secret)
  configs["fs.azure.account.oauth2.client.endpoint"] = "https://login.microsoftonline.com/{0}/oauth2/token"\
  .format(dbutils.secrets.get(scope = str_Enviroment_Scope, key = str_tenant_secret))

  try:
    dbutils.fs.mount(
    source = "abfss://" + str_mount_point + "@"+str_datalake_point+".dfs.core.windows.net/",
    mount_point = f"/mnt/{ALIAS_MOUNT}/"+str_mount_point,
    extra_configs = configs)
  except Exception as e:
    errorMsg = str(e)
    if "already mounted" in errorMsg :
      print("{} already mounted. Run previous cells to unmount first".format(str_mount_point))
    else:
      raise e

# COMMAND ----------

# DBTITLE 1,Function execution
#Se Ejecuta la funcion por cada contenedor que se desee crear un punto de montaje.
mount_point(ALIAS_MOUNT,MOUNT_CONTAINER_LANDING, LAKE_ACCOUNT, CLIENT_KEY, SECRET_KEY, TENANT_KEY, ENVIRONMENT_SCOPE, configs)
mount_point(ALIAS_MOUNT,MOUNT_CONTAINER_TRUSTED, LAKE_ACCOUNT, CLIENT_KEY, SECRET_KEY, TENANT_KEY, ENVIRONMENT_SCOPE, configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. _Validation options:_  

# COMMAND ----------

# DBTITLE 1,List secrets
# Comando para listar todos los ámbitos de secretos disponibles en tu workspace
secret_scopes = dbutils.secrets.listScopes()

# Imprimir los ámbitos
print("Ámbitos de secretos disponibles:")
for scope in secret_scopes:
    print(f"- {scope.name}")

# COMMAND ----------

# DBTITLE 1,list the keys
# Reemplaza 'nombre-de-tu-ambito' con el nombre real de tu ámbito de secretos
scope_name = "StorageScope" # Ejemplo

print(f"\nSecretos en el ámbito '{scope_name}':")
try:
    secrets_in_scope = dbutils.secrets.list(scope_name)
    for secret in secrets_in_scope:
        print(f"- {secret.key}")
except Exception as e:
    print(f"Error al listar secretos en el ámbito '{scope_name}': {e}")
    print("Asegúrate de que el ámbito existe y tienes los permisos de LECTURA adecuados.")

# COMMAND ----------

# DBTITLE 1,list created assemblies
mounts = dbutils.fs.mounts()

for mount in mounts:
    print("Mount Name: ", mount.mountPoint)
    print("Source: ", mount.source)
    # print("Options: ", mount.options)
    print("--------------------------------------------------")
#dbutils.fs.unmount("/mnt/bronze")
