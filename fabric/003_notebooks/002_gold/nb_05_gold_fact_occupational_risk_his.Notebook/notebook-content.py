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
# # Tabela: riscos.fact_occupational_risk_his
# ## Objetivo:
# Esta tabela contém os dados historicos da fact occupational risk
# 
# -------------
# 
# #### Histórico de alterações
# | Data | Desenvolvido por | Modificações |
# |---|---|---|
# | 01/04/2026 | Robson Mazzarotto| Criação do notebook |

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
table_name = 'fact_occupational_risk_his'

delta_table_name = f"{workspace}.{container}.{target_schema}.{table_name}"
 
delta_file = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{container}.Lakehouse/Tables/{target_schema}/{table_name}"
 
merge_columns = "OccupationalRiskHisId"
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
        S.UnitId                AS UnitId,
        S.Id                    AS OccupationalRiskHisId,
        S.RiscoOcupacional      AS OccupationalRiskId,

        S.ClasseRiscoDepois     AS Class,
        S.ProbabilidadeDepois   AS Probability,
        S.SeveridadeDepois      AS Severity,
        S.Status                AS Status,

        S.ClasseId              AS ClassId,
        S.ClassePosControleId   AS MitigatedClassId,
        S.ClasseLinearId        AS LinearClassId,
        S.ClasseLinearPosControleId AS LinearMitigatedClassId,
        S.RiscoLinear           AS LinearRiskScore,
        S.RiscoLinearPosControle AS MitigatedLinearRiskScore,

        1                       AS Active,
        S.Deletado              AS Eliminated,
        CAST(S.DataCriacao AS DATE) AS StartDate,
        CAST(NULL AS DATE)      AS EndDate,
        S.DataCriacao           AS CreateDateTime        
    FROM {workspace}.lh_silver.riscos.risco_auditoria AS S
""")

from pyspark.sql.functions import col
source_dataframe = source_dataframe.withColumn("EndDate", col("EndDate").cast("date"))

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

# MARKDOWN ********************

# ##### Atualiza coluna Active

# CELL ********************

spark.sql(f"""

MERGE INTO {workspace}.lh_gold.riscos.fact_occupational_risk_his AS FATO
USING (
    SELECT 
        OccupationalRiskId, 
        OccupationalRiskHisId,
        StartDate,
        LEAD(StartDate) OVER (
            PARTITION BY OccupationalRiskId 
            ORDER BY CreateDateTime ASC
        ) AS NextStartDate
    FROM {workspace}.lh_gold.riscos.fact_occupational_risk_his
    WHERE Active = 1
) AS L

ON FATO.OccupationalRiskHisId = L.OccupationalRiskHisId

WHEN MATCHED 
    AND L.NextStartDate IS NOT NULL
    AND FATO.Active = 1
    AND L.NextStartDate >= FATO.StartDate

THEN UPDATE SET
    FATO.Active = 0,
    FATO.EndDate = date_sub(L.NextStartDate, 1)

""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
