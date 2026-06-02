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
# # Tabela: riscos.dim_unit
# ## Objetivo:
# Esta tabela contém os possíveis resultados de um risco ocupacional
# 
# -------------
# 
# #### Histórico de alterações
# | Data | Desenvolvido por | Modificações |
# |---|---|---|
# | 31/03/2026 | Robson Mazzarotto| Criação do notebook |

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
table_name = 'dim_unit'

delta_table_name = f"{workspace}.{container}.{target_schema}.{table_name}"
 
delta_file = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{container}.Lakehouse/Tables/{target_schema}/{table_name}"
 
merge_columns = "UnitId"
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

WITH CTE AS (
    SELECT 
        S.Id,
        TRIM(Nivel) AS Nivel,
        ROW_NUMBER() OVER (PARTITION BY S.Id ORDER BY (SELECT NULL)) AS Posicao
    FROM {workspace}.lh_silver.riscos.unidade_idioma AS S
    LATERAL VIEW explode(split(S.Hierarquia, '>')) t AS Nivel
    WHERE S.IdiomaId = 1
),

PIVOTED AS (
    SELECT 
        Id,
        MAX(CASE WHEN Posicao = 1 THEN Nivel END) AS Level1,
        MAX(CASE WHEN Posicao = 2 THEN Nivel END) AS Level2,
        MAX(CASE WHEN Posicao = 3 THEN Nivel END) AS Level3,
        MAX(CASE WHEN Posicao = 4 THEN Nivel END) AS Level4,
        MAX(CASE WHEN Posicao = 5 THEN Nivel END) AS Level5,
        MAX(CASE WHEN Posicao = 6 THEN Nivel END) AS Level6,
        MAX(CASE WHEN Posicao = 7 THEN Nivel END) AS Level7,
        MAX(CASE WHEN Posicao = 8 THEN Nivel END) AS Level8
    FROM CTE
    GROUP BY Id
)

SELECT 
    S.Id AS UnitId,
    S.IdSuperior AS HigherUnitId,
    P.Level1,
    P.Level2,
    P.Level3,
    P.Level4,
    P.Level5,
    P.Level6,
    P.Level7,
    P.Level8,
    S.Latitude,
    S.Longitude,
    S.QtdeMapaLOTOVencido AS LOTOMapExpired,
    S.QtdeMapaSegurancaVencido AS SecurityMapExpired,
    S.QtdeRiscoOcupacionalCriticoSemPlanoAcao AS RiskWithoutAction
FROM {workspace}.lh_silver.riscos.unidade AS S
LEFT JOIN PIVOTED P ON S.Id = P.Id

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
