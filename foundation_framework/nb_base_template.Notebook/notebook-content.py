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

# ##### Parameters

# PARAMETERS CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Configuration and imports

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Import shared functions and setup logging

# CELL ********************

%run nb_shared_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

setup_logging()
logger = logging.getLogger('ADD NAME')  # Add name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Function Definitions

# MARKDOWN ********************

# ##### Main Execution Logic

# CELL ********************

metadata_db = SQLDatabase.from_metadata_control_db()

run_id = str(uuid.uuid4())

# Set variables
source_settings = source_settings or '{}'
target_settings = target_settings or '{}'
option_settings = option_settings or '{}'

source_settings = json.loads(source_settings)
target_file_path = source_settings.get('target_file_path')
target_file_name = source_settings.get('target_file_name')

target_settings = json.loads(target_settings)
schema_name = target_settings.get('schema_name')
table_name = target_settings.get('table_name')

option_settings = json.loads(option_settings)
record_path = option_settings.get('record_path')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Log start
log_start_parameters = {
    'run_id': run_id,
    'task_id': task_id,
    'workspace_id': notebookutils.runtime.context.get('currentWorkspaceId'),
    'executing_object_id': notebookutils.runtime.context.get('currentNotebookId'),
    'executing_object_name': notebookutils.runtime.context.get('currentNotebookName'),
    'executing_object_run_id': notebookutils.runtime.context.get('activityId'),
    'executing_object_type': 'Notebook'
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
