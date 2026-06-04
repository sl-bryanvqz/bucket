# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ##### Parameters

# PARAMETERS CELL ********************

source_settings = '{"source_name": "northwind", "resource": "Customers"}'
target_settings = '{"target_file_path": "northwind/default/customers", "target_file_name": "customers", "target_file_type": "json"}'
source_connection_settings = '{"authentication": "None", "base_url": "https://demodata.grapecity.com/northwind/odata/v1/"}'
lineage_id = '00000000-0000-0000-0000-000000000000'
task_executions_id = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ##### Configuration and Imports

# CELL ********************

import json
import fsspec
import nbformat
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ##### Import shared functions and set up logging

# CELL ********************

# %run nb_shared_functions Not supported in Python notebooks
nb_str = notebookutils.notebook.getDefinition('nb_shared_functions')
nb = nbformat.from_dict(json.loads(nb_str))
shell = get_ipython()
for cell in nb.cells:
    if cell.cell_type == 'code':
        code = ''.join(cell['source'])
        shell.run_cell(code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# %run nb_shared_functions Not supported in Python notebooks
nb_str = notebookutils.notebook.getDefinition('nb_rest_api_utility_functions')
nb = nbformat.from_dict(json.loads(nb_str))
shell = get_ipython()
for cell in nb.cells:
    if cell.cell_type == 'code':
        code = ''.join(cell['source'])
        shell.run_cell(code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

setup_logging()
logger = logging.getLogger('ExtractAPI')
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ##### Function definitions

# CELL ********************

def params_to_json(**kwargs):
    return json.dumps(kwargs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def resolve_config(config):
    resolved_config = config.copy()
    secret_fields = ['tenant_id', 'client_id', 'client_secret']
    if 'akv_uri' in resolved_config:
        for field in secret_fields:
            if field in config:
                secret_name = resolved_config[field]
                secret_value = notebookutils.credentials.getSecret(resolved_config['akv_uri'], secret_name)
                if secret_value:
                    resolved_config[field] = secret_value
                    print(f'Resolved {field} from environment')
                else:
                    print(f'Warning: Environment variable {secret_name} not found')
                    del resolved_config[field]
    else:
        print('Warning: No AKV URI supplied')

    return resolved_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ##### Main execution logic

# CELL ********************

metadata_db = SQLDatabase.from_metadata_control_db()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

source_settings = source_settings or '{}'
source_connection_settings = source_connection_settings or '{}'
target_settings = target_settings or '{}'

source_connection_settings = json.loads(source_connection_settings)
source_settings = json.loads(source_settings)
target_settings = json.loads(target_settings)
   
target_file_path = target_settings.get('target_file_path')
target_file_name = target_settings.get('target_file_name')
target_file_type = target_settings.get('target_file_type')

vl_guids = notebookutils.variableLibrary.getLibrary('vl_guids')
target_workspace_id = vl_guids.bronze_workspace_id
target_lakehouse_id = vl_guids.bronze_lakehouse_id
target_abfss_path = f'abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Files/incoming'

current_date = datetime.now().strftime('%Y-%m-%d')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

auth = MetadataDrivenAPIAuthenticator()
config = {**source_connection_settings, **source_settings}
resolved_config = resolve_config(config)

logger.info('Processing response...')
try:
    # Process the response data (which is usually JSON)
    data = auth.extract_data(resolved_config)

    # Create directory if needed
    if not notebookutils.fs.exists(f'{target_abfss_path}/{target_file_path}'):
        notebookutils.fs.mkdirs(f'{target_abfss_path}/{target_file_path}')

    # Write JSON
    logger.info('Writing to file...')
    output_file_path = f'{target_abfss_path}/{target_file_path}/{current_date}/lineage_id={lineage_id}/{target_file_name}.{target_file_type}'
    with fsspec.open(output_file_path, 'w') as f:
        json.dump(data, f, indent=2)

    log_data = params_to_json(
        base_url=source_connection_settings.get('base_url'),
        resource=source_settings.get('resource'),
        query_parameters=source_settings.get('query_parameters'),
        target_workspace_id=target_workspace_id,
        target_lakehouse_id=target_lakehouse_id,
        target_file_path=target_file_path,
        target_file_name=f'{target_file_name}.{target_file_type}'
    )

    sql = f'''
        UPDATE
            logging.task_executions
        SET
            log_data = '{log_data}'
        WHERE
            task_executions_id = '{task_executions_id}'
            AND log_data IS NULL
    '''

    metadata_db.execute_non_query(query=sql)

except Exception as e:
    logger.error(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

notebookutils.notebook.exit({"output_file_path": output_file_path})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
