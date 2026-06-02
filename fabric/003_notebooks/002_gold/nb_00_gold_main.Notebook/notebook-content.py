# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # ==========================================
# ## Utilização de Dags para orquestrar os notebooks
# # ==========================================

# CELL ********************

import json
from notebookutils import notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

notebook_name = ""
write_table_model = ""
workspace_id = ""
notebook_level = ""
batch_notebooks = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 1. RECEBER PARÂMETROS DO PIPELINE
# ============================================

notebook_name = notebook_name
write_table_model = write_table_model
workspace_id = workspace_id
notebook_level = notebook_level
batch_notebooks = batch_notebooks

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parse
notebooks_metadata = json.loads(batch_notebooks)
print(f"Total de notebooks recebidos: {len(notebooks_metadata)}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 2. SEPARAR POR NÍVEL
# ============================================
level_1 = [nb for nb in notebooks_metadata if nb.get("notebook_level") == "1"]
level_2 = [nb for nb in notebooks_metadata if nb.get("notebook_level") == "2"]

print(f"Level 1 (Dimensões): {len(level_1)} notebooks")
print(f"Level 2 (Fatos): {len(level_2)} notebooks\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 3. MONTAR DAG COM DEPENDÊNCIAS
# ============================================
activities = []
level_1_names = []  # Guardar nomes para as dependências

# 3.1 - Adicionar Level 1 (sem dependências - rodam em paralelo)
print("Preparando Level 1 (Dimensões):")
for nb in level_1:
    activity_name = f"dim_{nb['notebook_name']}"
    level_1_names.append(activity_name)
    
    activities.append({
        "name": activity_name,
        "path": nb['notebook_name'],
        "timeoutPerCellInSeconds": 600,
        "args": {
            "workspace_id": workspace_id,
            "write_table_model": nb['write_table_model']
        },
        "dependencies": []  # SEM dependências
    })
    print(f"  ✓ {nb['notebook_name']}")

# 3.2 - Adicionar Level 2 (dependem de TODOS do Level 1)
print(f"\nPreparando Level 2 (Fatos - dependem de {len(level_1_names)} dimensões):")
for nb in level_2:
    activity_name = f"fact_{nb['notebook_name']}"
    
    activities.append({
        "name": activity_name,
        "path": nb['notebook_name'],
        "timeoutPerCellInSeconds": 900,
        "args": {
            "workspace_id": workspace_id,
            "write_table_model": nb['write_table_model']
        },
        "dependencies": level_1_names  # DEPENDE DE TODAS AS DIMENSÕES
    })
    print(f"  ✓ {nb['notebook_name']} → aguarda {len(level_1_names)} dimensões")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 4. CRIAR DAG
# ============================================
DAG = {
    "activities": activities,
    "timeoutInSeconds": 7200,
    "concurrency": 10
}

print("\n" + "=" * 60)
print("EXECUTANDO COM DEPENDÊNCIAS")
print("=" * 60)
print(f"Total de atividades: {len(activities)}")
print(f"Dimensões (paralelo): {len(level_1)}")
print(f"Fatos (após dimensões): {len(level_2)}")
print("=" * 60 + "\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# ============================================
# 5. EXECUTAR
# ============================================
try:
    result = notebookutils.notebook.runMultiple(DAG)
    
    # Contar resultados
    success = sum(1 for r in result.values() if str(r.get('status', '')).lower() == 'succeeded')
    failed = len(result) - success
    
    print("\n" + "=" * 60)
    print("RESULTADO")
    print("=" * 60)
    
    # Mostrar cada resultado
    for activity_name, run_result in result.items():
        status = run_result.get('status', 'unknown')
        symbol = "✓" if str(status).lower() == 'succeeded' else "✗"
        print(f"{symbol} {activity_name}: {status}")
    
    print("=" * 60)
    print(f"✓ Sucesso: {success}")
    print(f"✗ Falhas: {failed}")
    print("=" * 60)
    
    if failed == 0:
        notebook.exit("SUCCESS")
    else:
        notebook.exit(f"PARTIAL_FAILURE: {failed} falhas")
        
except Exception as e:
    print(f"\n❌ ERRO: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# ============================================
# 5. EXECUTAR
# ============================================

result = notebookutils.notebook.runMultiple(DAG)

# Contar resultados
success = 0
failed = 0

print("\n" + "=" * 60)
print("RESULTADO DA EXECUÇÃO")
print("=" * 60)

for activity_name, run_result in result.items():
    exception = run_result.get('exception')
    exit_val = run_result.get('exitVal', '')
    
    if exception is None:
        success += 1
        print(f"✓ {activity_name}")
    else:
        failed += 1
        print(f"✗ {activity_name}")
        print(f"  Erro: {exception}")

print("=" * 60)
print(f"Total: {len(result)} notebooks")
print(f"✓ Sucesso: {success}")
print(f"✗ Falhas: {failed}")
print("=" * 60)

# Finalizar
if failed == 0:
    print("\n🎉 Todos os notebooks executados com sucesso!")
    notebook.exit("SUCCESS")
else:
    print(f"\n⚠️ {failed} notebook(s) falharam")
    notebook.exit(f"PARTIAL_FAILURE: {failed} falhas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
