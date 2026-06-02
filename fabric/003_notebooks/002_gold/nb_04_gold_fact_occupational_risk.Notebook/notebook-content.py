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
# # Tabela: riscos.fact_occupational_risk
# ## Objetivo:
# Esta tabela é o principal registro do sistema, representa um perigo aos colaboradores, identificado em um local, e avaliado com um resultado. É composto por: Unidade (o local da empresa), Perigo (o perigo identificado), N campos de Caracterização (campos de detalhamento do perigo), N campos de Variáveis (variáveis usadas para avaliar o risco), N Medidas de Controles (controles usados para mitigar o risco), Probabilidade e Severidade (resultados obtidos a partir das variáveis), Classe de Risco (obtidos a partir do cruzamento de Probabilidade e Severidade, antes dos controles e depois dos controles, sendo do tipo normal - matriz de risco - ou risco linear)
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
table_name = 'fact_occupational_risk'

delta_table_name = f"{workspace}.{container}.{target_schema}.{table_name}"
 
delta_file = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{container}.Lakehouse/Tables/{target_schema}/{table_name}"
 
merge_columns = "Id"
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

WITH BASE AS (
    SELECT 
        S.UnidadeId,
        S.Id,

        S.ClasseId,
        S.ClassePosControleId,
        S.ClasseLinearId,
        S.ClasseLinearPosControleId,
        S.RiscoLinear,
        S.RiscoLinearPosControle,

        S.ProbabilidadeId,
        S.ProbabilidadePosControleId,
        S.SeveridadeId,
        S.PerigoId,

        S.DataCriacao,
        S.DataAtualizacao,
        S.Deletado,
        S.Status
    FROM {workspace}.lh_silver.riscos.risco_ocupacional S
),

FINAL AS (
    SELECT 
        -- 🔑 PADRÃO DO FRAMEWORK
        Id,

        UnidadeId AS UnitId,

        ClasseId AS ClassId,
        ClassePosControleId AS MitigatedClassId,
        ClasseLinearId AS LinearClassId,
        ClasseLinearPosControleId AS LinearMitigatedClassId,
        RiscoLinear AS LinearRiskScore,
        RiscoLinearPosControle AS MitigatedLinearRiskScore,

        ProbabilidadeId AS ProbabilityId,
        ProbabilidadePosControleId AS MitigatedProbabilityId,
        SeveridadeId AS SeverityId,
        PerigoId AS HazardId,

        TO_DATE(DataCriacao) AS CreationDate,
        TO_DATE(DataAtualizacao) AS UpdateDate,

        DATE_ADD(COALESCE(DataAtualizacao, DataCriacao), 50) AS EvaluationExpirationDate,

        Deletado AS Eliminated,

        Status,

        -- 🔥 mantido para compatibilidade de schema (caso exista na gold)
        CAST(NULL AS INT) AS OccupationalRiskHisId

    FROM BASE
)

SELECT * FROM FINAL

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
