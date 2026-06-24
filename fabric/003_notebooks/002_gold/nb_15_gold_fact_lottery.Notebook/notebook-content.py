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

# # Tabela: riscos.fact_lottery
# ## Objetivo:
# Registro principal de um mapa de LOTO (Lockout-Tagout), contendo as informações de bloqueio e etiquetagem de fontes de energia para garantir segurança durante manutenção de equipamentos
# 
# -------------
# 
# #### Histórico de alterações
# | Data | Desenvolvido por | Modificações |
# |---|---|---|
# | 22/06/2026 | Robson Mazzarotto| Criação do notebook |

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
table_name = 'fact_lottery'

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
    UnidadeId,
    UnidadeHierarquia_ptBR,
    UnidadeHierarquia_noNO,
    UnidadeHierarquia_enUS,
    UnidadeHierarquia_esES,
    UnidadeHierarquia_itIT,
    UnidadeHierarquia_arSA,
    UnidadeHierarquia_svSV,
    UnidadeHierarquia_plPL,
    UnidadeHierarquia_csCS,
    UnidadeHierarquia_frFR,
    UnidadeHierarquia_thTH,
    UnidadeHierarquia_zhZH,
    UnidadeHierarquia,
    UnidadeDescricao_ptBR,
    UnidadeDescricao_noNO,
    UnidadeDescricao_enUS,
    UnidadeDescricao_esES,
    UnidadeDescricao_itIT,
    UnidadeDescricao_arSA,
    UnidadeDescricao_svSV,
    UnidadeDescricao_plPL,
    UnidadeDescricao_csCS,
    UnidadeDescricao_frFR,
    UnidadeDescricao_thTH,
    UnidadeDescricao_zhZH,
    Observacoes_ptBR,
    Observacoes_noNO,
    Observacoes_enUS,
    Observacoes_esES,
    Observacoes_itIT,
    Observacoes_arSA,
    Observacoes_svSV,
    Observacoes_plPL,
    Observacoes_csCS,
    Observacoes_frFR,
    Observacoes_thTH,
    Observacoes_zhZH,
    UrlImagem,
    Status,
    UsuarioEmissaoUsername,
    UsuarioEmissaoNome,
    DataEmissao,
    DataVencimento,
    CamposPersonalizados_ptBR,
    CamposPersonalizados_noNO,
    CamposPersonalizados_enUS,
    CamposPersonalizados_esES,
    CamposPersonalizados_itIT,
    CamposPersonalizados_arSA,
    CamposPersonalizados_svSV,
    CamposPersonalizados_plPL,
    CamposPersonalizados_csCS,
    CamposPersonalizados_frFR,
    CamposPersonalizados_thTH,
    CamposPersonalizados_zhZH,
    CamposPersonalizados,
    UnidadeDescricao,
    Observacoes,
    Deletado,
    LOTOId,
    Sequencia,
    CamposPersonalizados_deDE,
    Observacoes_deDE,
    UnidadeDescricao_deDE,
    UnidadeHierarquia_deDE,
    CamposPersonalizados_bgBG,
    Observacoes_bgBG,
    UnidadeDescricao_bgBG,
    UnidadeHierarquia_bgBG,
    CamposPersonalizados_roRO,
    Observacoes_roRO,
    UnidadeDescricao_roRO,
    UnidadeHierarquia_roRO,
    CamposPersonalizados_elEL,
    Observacoes_elEL,
    UnidadeDescricao_elEL,
    UnidadeHierarquia_elEL,
    CamposPersonalizados_trTR,
    Observacoes_trTR,
    UnidadeDescricao_trTR,
    UnidadeHierarquia_trTR,
    MapaNumero,
    UnidadeHierarquia_esEP,
    UnidadeDescricao_esEP,
    Observacoes_esEP,
    CamposPersonalizados_esEP,
    CamposPersonalizados_huHU,
    CamposPersonalizados_nlNL,
    CamposPersonalizados_skSK,
    CamposPersonalizados_ukUA,
    Observacoes_huHU,
    Observacoes_nlNL,
    Observacoes_skSK,
    Observacoes_ukUA,
    UnidadeDescricao_huHU,
    UnidadeDescricao_nlNL,
    UnidadeDescricao_skSK,
    UnidadeDescricao_ukUA,
    UnidadeHierarquia_huHU,
    UnidadeHierarquia_nlNL,
    UnidadeHierarquia_skSK,
    UnidadeHierarquia_ukUA,
    insert_date,
    update_date,
    hash_row
FROM {workspace}.lh_silver.riscos.loto

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
