# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Overview
# This notebook ingests metadata configurations provided through a YAML file and loads them into the metadata control database. It validates the YAML inputs, stages the validated records in an interim table, and then publishes them to the final `meta.task` table.


# CELL ********************

# Type here in the cell editor to add code!
path=f"{notebookutils.nbResPath}/builtin/config.yml"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_load_metadata_config_utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test JDBC connection with Metadata control db

# CELL ********************

_test_jdbc_connection(jdbc_url,driver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load config file to dataframe

# CELL ********************

df_current = yaml_to_stage_rows(path)
display(df_current)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run validation on the loaded configs

# CELL ********************

df_warning=final_validation(df_current)
warning_records = df_warning.count()

if warning_records>0:
    print(f'WARNING.. Issues with {warning_records} records.')
    display(df_warning)
else:
    print('All records passed validations.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load metadata to Control database

# CELL ********************

load_changes_to_metadatadb(df_current)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
