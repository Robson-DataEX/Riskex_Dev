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

# CELL ********************

# ============================================
# BIBLIOTECAS
# ============================================
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import *
from delta.tables import *

import requests
from pyspark.sql import Row
import time
import datetime, pandas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# FUNÇÃO: calculate_hash
# ============================================
def calculate_hash(df, exclude_columns=['insert_date', 'update_date', 'hash_row']):
    """
    Calcula hash MD5 das colunas do DataFrame para detectar mudanças.
    Exclui colunas de controle e a própria coluna hash.
    """
    hash_columns = [col for col in df.columns if col not in exclude_columns]
    
    df_with_hash = df.withColumn(
        "hash_row",
        md5(concat_ws('|', *[coalesce(col(c).cast("string"), lit("")) for c in hash_columns]))
    )
    
    return df_with_hash

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# FUNÇÃO: add_date_columns
# ============================================
def add_date_columns(source_dataframe, add_hash=True):
    """
    Adiciona colunas de controle de data e hash ao DataFrame.
    """
    df = source_dataframe\
        .withColumn("insert_date", date_format(current_timestamp() - expr('INTERVAL 3 HOURS'), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))\
        .withColumn("update_date", date_format(current_timestamp() - expr('INTERVAL 3 HOURS'), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    
    if add_hash:
        df = calculate_hash(df)
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ============================================
# FUNÇÃO: table_exists
# ============================================
def table_exists(delta_file):
    """
    Verifica se uma tabela existe no banco de dados ou se o Delta existe no path.
    """

    # Se forneceu delta_file, verifica se existe Delta no path
    if delta_file:
        try:
            spark.read.format("delta").load(delta_file)
            return True
        except:
            pass
    
    return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# FUNÇÃO: create_table
# ============================================
def create_table(source_dataframe, delta_table_name, delta_file, partition_columns, string_columns, table_columns, table_type, view_name):
    """
    Cria uma nova tabela Delta (managed ou external).
    ATUALIZADO: Usa Fully Qualified Name para schema
    """
    
    # Extrair schema com FQN (workspace.lakehouse.schema)
    parts = delta_table_name.split(".")
    
    if len(parts) >= 3:
        # Formato: workspace.lakehouse.schema.table
        # Pegar workspace.lakehouse.schema
        schema_fqn = ".".join(parts[:3])
    else:
        # Fallback: usar nome completo
        schema_fqn = delta_table_name
    
    schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}"

    partition = f"PARTITIONED BY ({partition_columns})" if partition_columns != 'N/A' else ''

    tbl_properties = """
    'delta.logRetentionDuration'='interval 7 days',
    'delta.deletedFileRetentionDuration'='interval 7 days',  
    'delta.enableChangeDataFeed'='true',
    'overwriteSchema'='true'
    """

    if table_type == 'managed':
        create_table_query = f"""
            CREATE OR REPLACE TABLE {delta_table_name} (
                {table_columns} 
            )
            USING delta
            {partition}
            TBLPROPERTIES ({tbl_properties})
        """
        print('Script Tabela Managed')
    else:
        create_table_query = f"""
            CREATE OR REPLACE TABLE {delta_table_name} (
                {table_columns} 
            )
            USING delta
            {partition}
            LOCATION '{delta_file}'
            TBLPROPERTIES ({tbl_properties})
        """
        print('Script Tabela External')

    print(f'Script Schema: {schema_query}')
    spark.sql(schema_query)
    
    print(f"Executando: {create_table_query}")
    spark.sql(create_table_query)

    print(f"Inserindo dados na tabela {delta_table_name}...")
    spark.sql(f"INSERT INTO {delta_table_name} ({string_columns}) SELECT * FROM {view_name}")

    return print(f"✓ Tabela {delta_table_name} criada e dados inseridos com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# FUNÇÃO: apply_merge 
# ============================================
def apply_merge(df, delta_file, table_name, merge_columns, delta_table_name, view_name):
    """
    Executa operação de MERGE na tabela Delta.
    ATUALIZADO: Recebe view_name para evitar conflito de sessão
    """
    
    # Ler tabela existente
    existing_table = spark.table(delta_table_name)
    existing_columns = [c.lower() for c in existing_table.columns]
    source_columns = [c.lower() for c in df.columns]
    
    # Verificar se tem hash_row
    has_hash_column = 'hash_row' in existing_columns
    
    # Preparar condição de merge
    merge_cols = [col.strip() for col in merge_columns.split(',')]
    merge_condition = " AND ".join([f"src.{col} = tgt.{col}" for col in merge_cols])
    
    # Adicionar hash na condição se disponível
    if has_hash_column and 'hash_row' in source_columns:
        merge_condition_full = f"{merge_condition} AND src.hash_row <> tgt.hash_row"
        print("✓ Utilizando hash_row para otimização do merge (evita full scan)")
    else:
        merge_condition_full = merge_condition
    
    # Preparar colunas para UPDATE (excluir insert_date e hash_row)
    update_columns = [col for col in df.columns if col.lower() not in ['insert_date', 'update_date', 'hash_row']]
    update_set_parts = [f"tgt.{col} = src.{col}" for col in update_columns]
    update_set_parts.append("tgt.update_date = current_timestamp()")
    update_set_sql = ", ".join(update_set_parts)
    
    # Preparar colunas para INSERT
    insert_columns = ", ".join(df.columns)
    insert_values = ", ".join([f"src.{col}" for col in df.columns])
    
    # MERGE query usando view_name (nome único)
    merge_query = f"""
        MERGE INTO {delta_table_name} AS tgt
        USING {view_name} AS src
        ON {merge_condition}
        WHEN MATCHED AND src.hash_row <> tgt.hash_row THEN 
            UPDATE SET {update_set_sql}
        WHEN NOT MATCHED THEN 
            INSERT ({insert_columns}) 
            VALUES ({insert_values})
    """
    
    print(f"\nExecutando MERGE:\n{merge_query}\n")

    if set(existing_columns) != set(source_columns):
        print(f'⚠ Estrutura da tabela {delta_table_name} foi alterada na origem. Rever os dados')
    else:
        result = spark.sql(merge_query)
    
    print(f'✓ Merge concluído com sucesso na tabela {delta_table_name}')
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# FUNÇÃO: automated_load (ATUALIZADA - usa view_name único)
# ============================================
def automated_load(source_dataframe, delta_file, table_name, delta_table_name, write_table_model, merge_columns, table_type='external', partition_columns='N/A', add_hash=True):
    """
    Função principal para carga automatizada de dados em tabelas Delta.
    ATUALIZADO: Cria view temporária com nome único para evitar conflitos em sessões compartilhadas
    
    Parâmetros:
    - source_dataframe: DataFrame com os dados de origem
    - delta_file: Caminho do arquivo Delta (para tabelas external)
    - table_name: Nome da tabela
    - delta_table_name: Nome completo da tabela (database.table)
    - write_table_model: Modo de escrita ('merge', 'overwrite', 'append')
    - merge_columns: Colunas para merge (separadas por '|')
    - table_type: Tipo da tabela ('external' ou 'managed')
    - partition_columns: Colunas de partição
    - add_hash: Se True, adiciona coluna hash (recomendado para merge)
    """
    
    # ============================================
    # CRIAR NOME ÚNICO PARA VIEW TEMPORÁRIA
    # ============================================
    # Remove caracteres especiais e cria nome único baseado na tabela
    view_name = f"tmp_source_{table_name.replace('.', '_').replace('-', '_')}"
    
    print(f"\n{'='*80}")
    print(f"INICIANDO CARGA: {delta_table_name}")
    print(f"View temporária: {view_name}")
    print(f"Modo: {write_table_model.upper()} | Tipo: {table_type.upper()}")
    print(f"{'='*80}\n") 
    
    exists = table_exists(delta_file)
    print(f"Tabela existe: {exists}")
    
    # ============================================
    # PREPARAR DATAFRAME
    # ============================================
    df = add_date_columns(source_dataframe, add_hash=add_hash)
    
    # LIMPAR view anterior se existir (importante para sessionTag)
    try:
        spark.catalog.dropTempView(view_name)
        print(f"✓ View anterior '{view_name}' limpa")
    except:
        pass
    
    # CRIAR view com nome único
    df.createOrReplaceTempView(view_name)
    print(f"✓ View '{view_name}' criada")
    
    string_columns = ', '.join(df.columns)
    table_columns = ', '.join([f'{c} {t}' for c, t in df.dtypes])
    
    print(f"Colunas no DataFrame: {len(df.columns)}")
    if add_hash:
        print("✓ Hash_row adicionado para otimização")
    
    # ============================================
    # LÓGICA PRINCIPAL DE CARGA
    # ============================================
    
    if not exists:
        create_table(df, delta_table_name, delta_file, partition_columns, string_columns, table_columns, table_type, view_name)

    elif write_table_model == 'merge':
        print("Executando MERGE (carga incremental)...")
        apply_merge(df, delta_file, table_name, merge_columns, delta_table_name, view_name)
    
    elif write_table_model == 'overwrite':
        print("Executando OVERWRITE (carga completa)...")
        
        if table_type == 'external':
            if partition_columns != 'N/A':
                df.write.option("overwriteSchema", "true").mode("overwrite").format('delta').partitionBy(partition_columns).saveAsTable(delta_table_name, path=delta_file)
            else:
                df.write.option("overwriteSchema", "true").mode("overwrite").format('delta').saveAsTable(delta_table_name, path=delta_file)
            print(f"✓ External Table - Overwrite concluído em {delta_table_name}")
        
        else:
            if partition_columns != 'N/A':
                df.write.option("overwriteSchema", "true").mode("overwrite").format('delta').partitionBy(partition_columns).saveAsTable(delta_table_name)
            else:
                df.write.option("overwriteSchema", "true").mode("overwrite").format('delta').saveAsTable(delta_table_name)
            print(f"✓ Managed Table - Overwrite concluído em {delta_table_name}")
    
    elif write_table_model == 'append':
        print("Executando APPEND (adiciona registros)...")
        
        if table_type == 'external':
            if partition_columns != 'N/A':
                df.write.option("mergeSchema", "true").mode("append").format('delta').partitionBy(partition_columns).saveAsTable(delta_table_name, path=delta_file)
            else:
                df.write.option("mergeSchema", "true").mode("append").format('delta').saveAsTable(delta_table_name, path=delta_file)
            print(f"✓ External Table - Append concluído em {delta_table_name}")
        
        else:
            if partition_columns != 'N/A':
                df.write.option("mergeSchema", "true").mode("append").format('delta').partitionBy(partition_columns).saveAsTable(delta_table_name)
            else:
                df.write.option("mergeSchema", "true").mode("append").format('delta').saveAsTable(delta_table_name)
            print(f"✓ Managed Table - Append concluído em {delta_table_name}")
    
    else:
        print(f'⚠ ERRO: Modo de carga "{write_table_model}" não reconhecido.')
        print('Modos válidos: merge, overwrite, append')
    
    # ============================================
    # LIMPEZA FINAL
    # ============================================
    # Limpar view após uso para liberar memória
    try:
        spark.catalog.dropTempView(view_name)
        print(f"✓ View '{view_name}' limpa após processamento")
    except:
        pass
    
    print(f"\nCARGA FINALIZADA: {delta_table_name}")
    print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

