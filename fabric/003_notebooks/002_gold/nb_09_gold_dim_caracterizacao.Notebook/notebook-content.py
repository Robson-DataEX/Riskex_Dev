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
# # Tabela: riscos.dim_caracterizacao
# ## Objetivo:
# Esta tabela contém o cadastro de campos configuráveis (campos extras - cada empresa define quais campos deseja utilizar) 
# 
# -------------
# 
# #### Histórico de alterações
# | Data | Desenvolvido por | Modificações |
# |---|---|---|
# | 16/06/2026 | Robson Mazzarotto| Criação do notebook |

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
table_name = 'dim_caracterizacao'

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

SELECT 
    Id,
    Descricao_ptBR,
    Descricao_noNO,
    Descricao_enUS,
    Descricao_esES,
    Descricao_itIT,
    Descricao_arSA,
    Descricao_svSV,
    Descricao_plPL,
    Descricao_csCS,
    Descricao_frFR,
    Descricao_thTH,
    Descricao_zhZH,
    Descricao,
    Tipo,
    Obrigatorio,
    CampoFormularioCustomizado,
    CampoRiscoAmbiental,
    CampoRiscoOcupacional,
    Ocupacional,
    Ambiental,
    FormularioCustomizado,
    PGRDefinicao,
    ModoComplexoTemplate,
    ModoLista,
    DataHoraCriacao,
    UsuarioCriacaoId,
    DataHoraAlteracao,
    UsuarioAlteracaoId,
    DataHoraExclusao,
    SYNC_DATE,
    PGRDefinicao_arSA,
    PGRDefinicao_csCS,
    PGRDefinicao_enUS,
    PGRDefinicao_esES,
    PGRDefinicao_frFR,
    PGRDefinicao_itIT,
    PGRDefinicao_noNO,
    PGRDefinicao_plPL,
    PGRDefinicao_ptBR,
    PGRDefinicao_svSV,
    PGRDefinicao_thTH,
    PGRDefinicao_zhZH,
    MultiSelecao,
    Descricao_deDE,
    PGRDefinicao_deDE,
    Descricao_bgBG,
    PGRDefinicao_bgBG,
    Descricao_roRO,
    PGRDefinicao_roRO,
    Descricao_elEL,
    PGRDefinicao_elEL,
    Descricao_trTR,
    PGRDefinicao_trTR,
    Descricao_esEP,
    PGRDefinicao_esEP,
    Descricao_huHU,
    Descricao_nlNL,
    Descricao_skSK,
    Descricao_ukUA,
    PGRDefinicao_huHU,
    PGRDefinicao_nlNL,
    PGRDefinicao_skSK,
    PGRDefinicao_ukUA,
    insert_date,
    update_date,
    hash_row
FROM {workspace}.lh_silver.riscos.caracterizacao

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
