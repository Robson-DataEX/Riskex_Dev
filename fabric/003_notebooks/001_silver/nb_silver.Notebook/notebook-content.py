# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## Configurações iniciais

# MARKDOWN ********************

# ### O que o código abaixo faz?
# 
# - Código abaixo configura o Microsoft Fabric Lakehouse como padrão dentro do notebook.
# - Define o lakehouse em questão como o padrão da sessão atual do notebook
# 
# 
# - Benefícios
#     - Automação: O workspace é detectado automaticamente e o lakehouse fica dinamico na criação do notebook.
#     - Essa abordagem evita a necessidade de fazer o Attach (inclusão) do lakehouse de maneira manual no notebook.

# MARKDOWN ********************

# ###### **Chamada no notebooks de funções**

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "lh_silver",
# MAGIC         "workspaceId":{
# MAGIC             "parameterName":"workspace_id",
# MAGIC             "defaultValue":""
# MAGIC         }
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

context = notebookutils.runtime.context
workspace = context['currentWorkspaceName']
defaultLakehouseName = context['defaultLakehouseName']
workspace_id = context['currentWorkspaceId']

print(workspace)
print(workspace_id)
print(defaultLakehouseName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Criando variáveis para receber os parâmetros**

# PARAMETERS CELL ********************

source_container = ""
source_directory = ""
source_file = ""
merge_columns = ""
target_container = ""
target_directory = ""
table_name = ""
partition_columns = ""
write_table_model = ""
apply_distinct = ""
table_type = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **armazenar variavéis enviadas pelo pipeline**

# CELL ********************

debug = False

if debug:

    # se debug = True, altere os parâmetros abaixo para testar a ingestão de dados.
    source_container = "lh_staging"
    source_directory = "DB_SALES_MSFABRIC"
    source_file = "Suppliers"
    merge_columns = "SupplierID"
    target_container = "lh_silver"
    target_directory = "sales"
    table_name = "Suppliers"
    partition_columns = "N/A"
    write_table_model = "merge" ## "overwrite" ou "replace_where" ou "merge" 
    apply_distinct = "1"
    table_type = "managed" #managed ou extenal

else:
    source_container = source_container
    source_directory = source_directory
    source_file = source_file
    merge_columns = merge_columns
    target_container = target_container
    target_directory = target_directory
    table_name = table_name
    partition_columns = partition_columns
    write_table_model = write_table_model
    apply_distinct = apply_distinct
    table_type = table_type

# Adequação e compilação de parámetros
path_source = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{source_container}.Lakehouse/Files/{source_directory}/{source_file}.parquet"

# teste com coluna a mais
# path_source = f"abfss://{workspace}@{data_lake_name}.dfs.core.windows.net/{source_directory}/evento1/*.parquet"

#delta_table_name = f"{target_container}.{table_name}"
delta_table_name = f"{workspace}.{target_container}.{target_directory}.{table_name}"

target_directory = target_directory.lower()
table_name = table_name.lower()

if table_type == 'external':
    #delta_file = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{target_container}.Lakehouse/Files/{delta_table_name}"
    delta_file = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{target_container}.Lakehouse/Files/{target_directory}/{delta_table_name}"
else:
    delta_file = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{target_container}.Lakehouse/Tables/{target_directory}/{table_name}"

print("source_container:", source_container)
print("source_directory:", source_directory)
print("source_file:", source_file)
print("merge_columns:", merge_columns)
print("target_container:", target_container)
print("target_directory:", target_directory)
print("table_name:", table_name)
print("delta_table_name:", delta_table_name)
print("partition_columns:", partition_columns)
print("write_table_model:", write_table_model)
print("apply_distinct:", apply_distinct)
print("table_type:", table_type)
print("formatted_source_directory(path_source):", path_source)
print("formatted_target_directory(delta_file):", delta_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Preparação dos Dados

# CELL ********************

# Leitura do arquivo parquet na transient
source_dataframe = spark.read.parquet(f'{path_source}')
source_dataframe_columns = source_dataframe.columns


# Verifica se deve remove as duplicadas no DataFrame
if apply_distinct  == '1':
    source_dataframe = source_dataframe.distinct()
    print("Deduplicação aplicada com sucesso.")
else:
    print("Deduplicação pass.")
    pass

# Ajusta os nomes das colunas
#source_dataframe = source_dataframe.toDF(*[c.lower() for c in source_dataframe.columns])
source_dataframe = source_dataframe.select([F.col(col).alias(col.replace(' ', '_')) for col in source_dataframe.columns])
source_dataframe = source_dataframe.select([F.col(col).alias(col.replace('/', '_')) for col in source_dataframe.columns])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Carga e atualização dos dados

# CELL ********************

if source_dataframe.count() > 0:
    automated_load(source_dataframe, delta_file, table_name, delta_table_name, write_table_model, merge_columns, table_type, partition_columns)

else:
    notebookutils.notebook.exit('O dataframe de origem está vazio e não há dados para carga/atualização.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
