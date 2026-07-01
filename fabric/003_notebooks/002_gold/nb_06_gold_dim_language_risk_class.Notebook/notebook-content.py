# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# 
# # Tabela: riscos.dim_language_risk_class
# ## Objetivo:
# Esta tabela contém as linguagem em Portugues, Espanhol e Inglês
# 
# -------------
# 
# #### Histórico de alterações
# | Data | Desenvolvido por | Modificações |
# |---|---|---|
# | 02/04/2026 | Robson Mazzarotto| Criação do notebook |

# MARKDOWN ********************

# ## Configurações Iniciais

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "lh_gold",
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

# CELL ********************

%run nb_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

write_table_model = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Target Delta Table Variables
 
container = 'lh_gold'
target_schema = 'riscos'
table_name = 'dim_language_risk_class'

delta_table_name = f"{workspace}.{container}.{target_schema}.{table_name}"
 
delta_file = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{container}.Lakehouse/Tables/{target_schema}/{table_name}"
 
merge_columns = "Id,IdiomaId"
partition_columns = 'N/A'
table_type = 'managed' #managed or external

write_table_model = write_table_model ## "overwrite" ou "append" ou "merge"


print(f"Tabela Delta: {delta_table_name}")
print(f"Path Delta: {delta_file}")
print(f"Colunas de Merge: {merge_columns}")
print(f"Tipo de Tabela: {table_type}")
print(f"Tipo de escrita da tabela: {write_table_model}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Preparação dos dados

# CELL ********************

source_dataframe = spark.sql(f"""

SELECT
    Id,
    IdiomaId,
    Descricao,
    Criterio,
    ClasseRiscoIdiomaId,
    insert_date,
    update_date,
    hash_row
FROM {workspace}.lh_silver.riscos.classe_risco_idioma
/*SELECT 
    S.Id,

    MAX(CASE WHEN S.IdiomaId = 1 THEN S.Descricao END) AS ptBR,
    MAX(CASE WHEN S.IdiomaId = 2 THEN S.Descricao END) AS enUS,
    MAX(CASE WHEN S.IdiomaId = 3 THEN S.Descricao END) AS esES,

    CASE 
        WHEN S.Id IN (13,14,15) THEN 'Unacceptable'
        ELSE 'Acceptable'
    END AS Tolerability

FROM {workspace}.lh_silver.riscos.classe_risco_idioma S
WHERE S.Id IN (9,12,13,14,15)
GROUP BY S.Id
*/

""")

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
