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

bronze_workspace_id = None
bronze_lakehouse_id = None
akv_uri = None
tenant_id_secret_name = None
client_id_secret_name = None
client_secret_name = None
server = None
database = None
parent_run_id = None
bronze_runs_to_execute = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Configuration and imports

# CELL ********************

import sys

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
logger = logging.getLogger('ParallelizeLoadToBronze')
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Function Definitions

# CELL ********************

def deduplicate_tasks_with_pairs(task_list):
    """
    Deduplicate a list of task dictionaries by task_id.
    For duplicate task_ids, create a list of dicts with (task_log_id, lineage_id) pairs.
    
    Args:
        task_list: List of dictionaries with task information
        
    Returns:
        List of deduplicated task dictionaries with log_run_pairs for duplicates
    """
    from collections import defaultdict
    
    # Group by task_id
    grouped_by_task_id = defaultdict(list)
    for item in task_list:
        task_id = item['task_id']
        grouped_by_task_id[task_id].append(item)
    
    # Create deduplicated list
    deduplicated_list = []
    
    for task_id, items in grouped_by_task_id.items():
        if False: #len(items) == 1:
            # Single item, keep as-is
            deduplicated_list.append(items[0])
        else:
            # Multiple items, create deduplicated entry with paired associations
            base_item = items[0].copy()  # Use first item as base
            
            # Remove individual task_log_id and lineage_id since we have pairs
            del base_item['previous_task_executions_id']
            del base_item['previous_lineage_id']
            
            # Create list of dicts with (task_log_id, lineage_id) pairs
            log_run_pairs = [
                {
                    'previous_task_executions_id': item['previous_task_executions_id'], 
                    'previous_lineage_id': item['previous_lineage_id']
                } 
                for item in items
            ]
            
            base_item['log_run_pairs'] = str(log_run_pairs)
            base_item['akv_uri'] = akv_uri
            base_item['tenant_id_secret_name'] = tenant_id_secret_name
            base_item['client_id_secret_name'] = client_id_secret_name
            base_item['client_secret_name'] = client_secret_name
            base_item['server'] = server
            base_item['database'] = database   
            base_item['parent_run_id'] = parent_run_id                                                            

            deduplicated_list.append(base_item)
    
    return deduplicated_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Main Execution Logic

# CELL ********************

notebook_name = 'nb_load_to_bronze'
notebook_path = notebook_name

# As of October 2025, SPN's cannot access Variable Libraries through Notebooks. Thus, this section is commented out. In the event that 
# Microsoft fixes this issue, the code can be re-instated
# vl_guids = notebookutils.variableLibrary.getLibrary('vl_guids')
# bronze_workspace_id = vl_guids.bronze_workspace_id
# bronze_lakehouse_id = vl_guids.bronze_lakehouse_id

lakehouse_path = f'abfss://{bronze_workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    logger.info('Getting file directories that need to loaded to bronze...')

    task_metadata = eval(bronze_runs_to_execute.replace("null",'None'))
    logger.info(f'Running {len(task_metadata)} load_to_bronze jobs:')
    print(task_metadata)

    if not task_metadata:
        notebookutils.notebook.exit('No files to process')
    deduplicated_task_metadata = deduplicate_tasks_with_pairs(task_metadata)

    logger.info('Building DAG...')
    activities = []
    for i, task in enumerate(deduplicated_task_metadata):
        activities.append(
            {
                "name": task['task_name'],
                "path": notebook_path,
                "timeoutPerCellInSeconds": 600,
                "args": task
            }
        )

    DAG = {
        "activities": activities,
        "timeoutInSeconds": 7200,
        "concurrency": 50
    }

    logger.info(f"Executing DAG for {len(DAG['activities'])} notebooks in parallel...")
    try:
        exit_values = notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": False})
        logger.info('All notebooks completed successfully')

    except Exception as run_exception:
        exit_values = run_exception.result

        failed_notebooks = []
        successful_notebooks = []

        # Map index back to activity names
        for activity_name, exit_value in exit_values.items():
            
            # Check if it failed
            if exit_value.get('exception') is not None:
                failed_notebooks.append({
                    'name': activity_name,
                    'exit_value': exit_value.get('exitVal'),
                    'error': str(exit_value.get('exception'))
                })
                logger.error(f"Notebook '{activity_name}' failed: {exit_value.get('exception')}")
            else:
                successful_notebooks.append(activity_name)
                logger.info(f"Notebook '{activity_name}' completed successfully")
        
        # Log summary
        logger.error(f"Failed: {len(failed_notebooks)} notebook(s)")
        logger.info(f"Successful: {len(successful_notebooks)} notebook(s)")

        raise

        ## Clean up empty directories
    logger.info('Cleaning up empty directories...')
    directory_maintenance = DirectoryMaintenance()

    root = f'{lakehouse_path}/Files/incoming/'
    directory_maintenance.delete_empty_directories(root, True)

    root = f'{lakehouse_path}/Files/processing/'
    directory_maintenance.delete_empty_directories(root, True)
    
except Exception as e:
    print(e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
