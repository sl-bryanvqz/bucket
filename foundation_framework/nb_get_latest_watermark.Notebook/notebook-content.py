# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# The notebook is used to extract the max watermark value from the bronze layer.

# PARAMETERS CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Example Parameters
task_id = 44
bronze_workspace_id = 'ffa9e649-af87-4d87-9af8-0b34250e695c'
bronze_lakehouse_id = 'fdfa0a43-11f0-4f2d-be5b-26ab506452e4'
target_file_path = 'archive/sapcrm/crm_jcds/2026-01-19/lineage_id=4c668c8c-4f75-452d-bf2b-3f51a1e449cc'
target_file_name = 'crm_jcds_.parquet'
incremental_field = 'order_date'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Libraries

# CELL ********************

import json
from notebookutils import mssparkutils
from decimal  import Decimal


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_path = f'abfss://{bronze_workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}'
file_full_path = f'{lakehouse_path}/Files/{target_file_path}/{target_file_name}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# spark.read.options(**read_options).parquet(file_path)
df = spark.read.parquet(file_full_path)
df.createOrReplaceTempView(f'temp_{task_id}')
max_val = spark.sql(f'Select max({incremental_field}) as max_val from temp_{task_id}').first()["max_val"]
payload = {"task_id": task_id, "max_incremental": int(max_val) if(max_val,Decimal) else max_val}
payload

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(json.dumps(payload))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
