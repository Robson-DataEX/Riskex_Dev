# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "71cb459b-4378-4622-b729-02fe38006a6e",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "b248fd3e-e82e-4081-b9c3-84074fb5659a",
# META       "known_lakehouses": [
# META         {
# META           "id": "71cb459b-4378-4622-b729-02fe38006a6e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Tratamento dos das na camada silver

# CELL ********************

import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import re
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("USE riscos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

statusplano_df = spark.read.json("Files/statusPlano.json", multiLine=True)

statusplano_df.write.mode("overwrite")\
    .option("overwriteSchema", "true") \
    .saveAsTable("status_plano")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

statusacao_df = spark.read.json("Files/statusAcao.json", multiLine=True)

statusacao_df.write.mode("overwrite")\
    .option("overwriteSchema", "true") \
    .saveAsTable("status_acao")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_unidade = spark.read.table("unidade_idioma")


df_unidade = df_unidade.withColumn("Hierarquia", concat_ws(" > ", df_unidade["Hierarquia"], df_unidade["Descricao"]))


df_unidade.write.mode("overwrite").saveAsTable("unidade_idioma")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import get_json_object, col

df_risco_auditoria = spark.read.table("risco_auditoria")
classe_risco_idioma = spark.read.table("classe_risco_idioma")

classe_risco_idioma_severidade = classe_risco_idioma.withColumnRenamed("Descricao", "Descricao_Severidade")
classe_risco_idioma_classe = classe_risco_idioma.withColumnRenamed("Descricao", "Descricao_Classe")
classe_risco_idioma_probabilidade = classe_risco_idioma.withColumnRenamed("Descricao", "Descricao_Probabilidade")

df_substituido = df_risco_auditoria \
    .join(classe_risco_idioma_severidade, \
          (df_risco_auditoria["SeveridadeDepois"] == classe_risco_idioma_severidade["Id"]) & \
          (classe_risco_idioma_severidade["IdiomaId"] == 1), "left") \
    .join(classe_risco_idioma_classe, \
          (df_risco_auditoria["ClasseRiscoDepois"] == classe_risco_idioma_classe["Id"]) & \
          (classe_risco_idioma_classe["IdiomaId"] == 1), "left") \
    .join(classe_risco_idioma_probabilidade, \
          (df_risco_auditoria["ProbabilidadeDepois"] == classe_risco_idioma_probabilidade["Id"]) & \
          (classe_risco_idioma_probabilidade["IdiomaId"] == 1), "left") \
    .select( \
        df_risco_auditoria["*"],
        classe_risco_idioma_severidade["Descricao_Severidade"],
        classe_risco_idioma_classe["Descricao_Classe"],
        classe_risco_idioma_probabilidade["Descricao_Probabilidade"]
    )

#adicionar colunas do V1 a V15 do Json e da C1 a C40 do Json e levar até a silver

df_substituido = df_substituido \
    .withColumn("classeLinearId", get_json_object(col("RegistroDepois"), "$.classeLinearId").cast("int")) \
    .withColumn("classeLinearPosControleId", get_json_object(col("RegistroDepois"), "$.classeLinearPosControleId").cast("int")) \
    .withColumn("riscoLinear", get_json_object(col("RegistroDepois"), "$.riscoLinear").cast("int")) \
    .withColumn("riscoLinearPosControle", get_json_object(col("RegistroDepois"), "$.riscoLinearPosControle").cast("int")) \
    .withColumn("classeId", get_json_object(col("RegistroDepois"), "$.classeId").cast("int")) \
    .withColumn("classePosControleId", get_json_object(col("RegistroDepois"), "$.classePosControleId").cast("int"))\
    .withColumn("Deletado", get_json_object(col("RegistroDepois"), "$.deletado").cast("BOOLEAN"))\
    .withColumn("status", get_json_object(col("RegistroDepois"), "$.status").cast("int"))  

# Adiciona colunas v1 a v15
for i in range(1, 16):
    df_substituido = df_substituido.withColumn(f"v{i}", get_json_object(col("RegistroDepois"), f"$.v{i}").cast("int"))

# Adiciona colunas c1 a c40
for i in range(1, 41):
    df_substituido = df_substituido.withColumn(f"c{i}", get_json_object(col("RegistroDepois"), f"$.c{i}").cast("int"))

# Remove colunas desnecessárias
df_substituido = df_substituido.drop(
    "Descricao_Severidade", 
    "Descricao_Classe", 
    "Descricao_Probabilidade", 
    "RegistroAntes", 
    "RegistroDepois", 
    "Detalhes"
)

# Lista base de colunas existentes
colunas_base = [
    "Id",
    "RiscoOcupacional",
    "RiscoAmbiental",
    "Datahora",
    "DataCriacao",
    "Usuario",
    "Justificativa",
    "Diff",

    "ClasseLinearId",
    "ClasseLinearPosControleId",
    "RiscoLinear",
    "RiscoLinearPosControle",
    "ClasseId",
    "ClassePosControleId",

    "ProbabilidadeAntes",
    "SeveridadeAntes",
    "ClasseRiscoAntes",
    "ProbabilidadeDepois",
    "SeveridadeDepois",
    "ClasseRiscoDepois",
    "EvidenciaId",
    "Tipo",
    "Deletado",
    "Status",
    "SYNC_DATE",
    "MobileId",
    "Motivo"
   # "extracted_at"
]

# Adiciona dinamicamente v1 a v15 e c1 a c40
colunas_v = [f"v{i}" for i in range(1, 16)]
colunas_c = [f"c{i}" for i in range(1, 41)]

# Combina todas as colunas
todas_colunas = colunas_base + colunas_v + colunas_c

# Seleciona todas as colunas no DataFrame final
df_final = df_substituido.select(*todas_colunas)

# df_substituido
#df_substituido.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.risco_auditoria")
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("risco_auditoria")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.functions import get_json_object, col

# ============================================================
# 🔹 LEITURA DAS TABELAS (BRONZE)
# ============================================================
df_risco_auditoria = spark.read.table("risco_auditoria")
classe_risco_idioma = spark.read.table("classe_risco_idioma")

df_risco_ocupacional = spark.read.table("risco_ocupacional")


# ============================================================
# 🔹 PREPARAÇÃO DAS TABELAS DE IDIOMA
# ============================================================
classe_risco_idioma_severidade = classe_risco_idioma.withColumnRenamed("Descricao", "Descricao_Severidade")
classe_risco_idioma_classe = classe_risco_idioma.withColumnRenamed("Descricao", "Descricao_Classe")
classe_risco_idioma_probabilidade = classe_risco_idioma.withColumnRenamed("Descricao", "Descricao_Probabilidade")


# ============================================================
# 🔹 JOIN PARA ENRIQUECIMENTO DE DESCRIÇÕES
# ============================================================
df_substituido = df_risco_auditoria \
    .join(classe_risco_idioma_severidade, \
          (df_risco_auditoria["SeveridadeDepois"] == classe_risco_idioma_severidade["Id"]) & \
          (classe_risco_idioma_severidade["IdiomaId"] == 1), "left") \
    .join(classe_risco_idioma_classe, \
          (df_risco_auditoria["ClasseRiscoDepois"] == classe_risco_idioma_classe["Id"]) & \
          (classe_risco_idioma_classe["IdiomaId"] == 1), "left") \
    .join(classe_risco_idioma_probabilidade, \
          (df_risco_auditoria["ProbabilidadeDepois"] == classe_risco_idioma_probabilidade["Id"]) & \
          (classe_risco_idioma_probabilidade["IdiomaId"] == 1), "left") \
    .select( \
        df_risco_auditoria["*"],
        classe_risco_idioma_severidade["Descricao_Severidade"],
        classe_risco_idioma_classe["Descricao_Classe"],
        classe_risco_idioma_probabilidade["Descricao_Probabilidade"]
    )


# ============================================================
# 🔹 JOIN COM RISCO_OCUPACIONAL
# ============================================================
df_risco_ocupacional_sel = df_risco_ocupacional.select(
    col("Id").alias("Id_risco_ocupacional"),
    col("UnidadeId")
)

df_substituido = df_substituido.join(
    df_risco_ocupacional_sel,
    df_substituido["RiscoOcupacional"] == col("Id_risco_ocupacional"),
    "left"
)

df_substituido = df_substituido.withColumn("UnitId", col("UnidadeId"))

df_substituido = df_substituido.drop("Id_risco_ocupacional", "UnidadeId")


# ============================================================
# 🔹 EXTRAÇÃO DE CAMPOS DO JSON
# ============================================================
df_substituido = df_substituido \
    .withColumn("ClasseLinearId", get_json_object(col("RegistroDepois"), "$.classeLinearId").cast("int")) \
    .withColumn("ClasseLinearPosControleId", get_json_object(col("RegistroDepois"), "$.classeLinearPosControleId").cast("int")) \
    .withColumn("RiscoLinear", get_json_object(col("RegistroDepois"), "$.riscoLinear").cast("int")) \
    .withColumn("RiscoLinearPosControle", get_json_object(col("RegistroDepois"), "$.riscoLinearPosControle").cast("int")) \
    .withColumn("ClasseId", get_json_object(col("RegistroDepois"), "$.classeId").cast("int")) \
    .withColumn("ClassePosControleId", get_json_object(col("RegistroDepois"), "$.classePosControleId").cast("int"))\
    .withColumn("Deletado", get_json_object(col("RegistroDepois"), "$.deletado").cast("BOOLEAN"))\
    .withColumn("Status", get_json_object(col("RegistroDepois"), "$.status").cast("int"))  


# ============================================================
# 🔹 EXTRAÇÃO DINÂMICA V1 A V15
# ============================================================
for i in range(1, 16):
    df_substituido = df_substituido.withColumn(
        f"v{i}",
        get_json_object(col("RegistroDepois"), f"$.v{i}").cast("int")
    )


# ============================================================
# 🔹 EXTRAÇÃO DINÂMICA C1 A C40
# ============================================================
for i in range(1, 41):
    df_substituido = df_substituido.withColumn(
        f"c{i}",
        get_json_object(col("RegistroDepois"), f"$.c{i}").cast("int")
    )


# ============================================================
# 🔹 LIMPEZA DE COLUNAS
# ============================================================
df_substituido = df_substituido.drop(
    "Descricao_Severidade", 
    "Descricao_Classe", 
    "Descricao_Probabilidade", 
    "RegistroAntes", 
    "RegistroDepois", 
    "Detalhes"
)


# ============================================================
# 🔹 SCHEMA FINAL
# ============================================================
colunas_base = [
    "Id",
    "RiscoOcupacional",
    "UnitId",
    "RiscoAmbiental",
    "Datahora",
    "DataCriacao",
    "Usuario",
    "Justificativa",
    "Diff",

    "ClasseLinearId",
    "ClasseLinearPosControleId",
    "RiscoLinear",
    "RiscoLinearPosControle",
    "ClasseId",
    "ClassePosControleId",

    "ProbabilidadeAntes",
    "SeveridadeAntes",
    "ClasseRiscoAntes",
    "ProbabilidadeDepois",
    "SeveridadeDepois",
    "ClasseRiscoDepois",
    "EvidenciaId",
    "Tipo",
    "Deletado",
    "Status",
    "SYNC_DATE",
    "MobileId",
    "Motivo"
]


# ============================================================
# 🔹 COLUNAS DINÂMICAS
# ============================================================
colunas_v = [f"v{i}" for i in range(1, 16)]
colunas_c = [f"c{i}" for i in range(1, 41)]

todas_colunas = colunas_base + colunas_v + colunas_c


# ============================================================
# 🔹 DATAFRAME FINAL
# ============================================================
df_final = df_substituido.select(*todas_colunas)


# ============================================================
# 🔹 ESCRITA
# ============================================================
df_final.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("risco_auditoria")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
