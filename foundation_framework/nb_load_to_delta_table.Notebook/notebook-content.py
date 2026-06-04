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

# Example Parameters
task_id = 44
task_name = 'Load to bronze - AW Address'
source_settings = '{"target_file_path": "sqldb-adventureworks/SalesLT/Address", "target_file_name": "Address", "target_file_type": "parquet"}'
target_settings = '{"schema_name": "sqldb_adventureworks", "table_name": "SalesLT__Address"}'
option_settings = None
log_run_pairs = "[{'previous_task_executions_id': 'E931F924-D316-4ED4-9EA6-FD961C1515D3', 'previous_lineage_id': 'E149B7C3-0410-479C-924D-72969FB58F15'}]"
bronze_workspace_id = '517f30b9-25fc-4bb5-9e45-dff29f29e706'
bronze_lakehouse_id = '563f991e-8460-4c35-a704-d39553aa8154'
akv_uri = None
tenant_id_secret_name = None
client_id_secret_name = None
client_secret_name = None
server = None
database = None
parent_run_id = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set Spark settings to enable reading of old dates
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead","CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite","CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Configuration and imports

# CELL ********************

import re
import ast
import json
import uuid
import pandas as pd
from pathlib import Path
from typing import List, Tuple, Dict, Any
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

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

%run nb_json_utility_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

setup_logging()
logger = logging.getLogger('LoadToBronze')
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Function Definitions

# CELL ********************

def list_files(path: str) -> List[Dict[str, Any]]:
    """Recursively list all files in a directory"""
    file_info = []
    try:
        objects = notebookutils.fs.ls(path)
        for obj in objects:
            try:
                if obj.isDir:
                    subdirectory_files = list_files(obj.path)
                    file_info.extend(subdirectory_files)
                else:
                    file_record = {
                        'file_name': obj.name,
                        'file_path': obj.path,
                        'file_size': obj.size,
                        'file_modify_time': obj.modifyTime
                    }
                    file_info.append(file_record)

            except Exception as e:
                # Continue processing other files if one fails
                logger.warning(f"Skipping {getattr(obj, 'path', 'unknown')}: {e}")
                continue

        # Sort by file_modify_time (oldest first)
        file_info.sort(key=lambda x: x['file_modify_time'], reverse=False)

        return file_info

    except Exception as e:
        logger.error(f'Error scanning directory {path}: {e}')
        return []


def move_file(source_prefix: str, source_path: str, destination_prefix: str) -> Tuple[str, str]:
    """Move file from source to destination with proper error handling"""

    # Normalize paths (fix trailing slash inconsistency)
    source_prefix = source_prefix.rstrip("/")
    destination_prefix = destination_prefix.rstrip("/")

    # Validate and extract relative path (more robust than replace)
    prefix_with_slash = f"{source_prefix}/"
    if not source_path.startswith(prefix_with_slash):
        raise ValueError(f"Source path must start with '{prefix_with_slash}'")


    relative_path = source_path[len(prefix_with_slash):]
    destination_path = f"{destination_prefix}/{relative_path}"
    destination_directory = destination_path.rstrip('/').rsplit('/', 1)[0]

    try:
        # Create destination directory
        notebookutils.fs.mkdirs(destination_directory)

        # Remove existing file if present
        if notebookutils.fs.exists(destination_path):
            notebookutils.fs.rm(destination_path, True)

        # Move file
        notebookutils.fs.mv(source_path, destination_path)

        # Improved logging
        display_path = destination_path.split('/Files/')[-1]
        logger.info(f"Moved file to /Files/{display_path}")

        return destination_directory, destination_path

    except Exception as e:
        logger.error(f"Failed to move {source_path} to {destination_path}: {e}")
        raise


def move_between_stages(source_prefix: str, destination_prefix: str) -> List[Tuple[str, str]]:
    """
    Move all files from source directory to destination directory.

    Args:
        source_prefix: Source directory path (e.g., 'incoming')
        destination_prefix: Destination directory path (e.g., 'processed')

    Returns:
        List of tuples containing (destination_directory, destination_path) for successfully moved files

    Raises:
        ValueError: If source or destination prefix is invalid
    """
    try:
        incoming_files = list_files(source_prefix)
    except Exception as e:
        logger.error(f"Failed to list files in {source_prefix}: {e}")
        return []

    if not incoming_files:
        logger.info(f"No files found in {source_prefix}")
        return []

    logger.info(f"Moving {len(incoming_files)} files from {source_prefix} to {destination_prefix}")

    successfully_moved = []
    failed_count = 0

    for file in incoming_files:
        try:
            destination_directory, destination_path = move_file(
                source_prefix,
                file.get('file_path'),
                destination_prefix
            )

            successfully_moved.append((destination_directory, destination_path))

        except Exception as e:
            # Log other errors but continue processing remaining files
            logger.error(f"Failed to move {file.get('file_path')}: {e}")
            failed_count += 1
            continue

    # Summary logging
    total_files = len(incoming_files)
    success_count = len(successfully_moved)

    if success_count > 0:
        logger.info(f"Successfully moved {success_count}/{total_files} files")

    if failed_count > 0:
        logger.warning(f"Failed to move {failed_count}/{total_files} files")

    return successfully_moved
    # Need to update state table and potentially log event


def read_single_file(lakehouse_path: str, file_path: str, file_format: str, handler: 'JsonFileHandler' = None, **read_options):
    """
    Read a single file into a Spark DataFrame.

    Args:
        lakehouse_path: Base lakehouse path for path transformations
        file_path: Full path to the file to read
        file_format: File format ('csv', 'parquet', 'json')
        handler: JsonFileHandler instance for JSON processing (flattening, validation, type conversion)
        **read_options: Additional options passed to Spark reader

    Returns:
        Spark DataFrame containing the file data

    Raises:
        ValueError: If file format is unsupported or parameters are invalid
        FileNotFoundError: If file doesn't exist
        Exception: If file reading fails
    """

    # Normalize file format
    file_format_lower = file_format.lower().strip()

    if file_format_lower == 'csv':
        csv_options = {"header": "true", **read_options}
        return spark.read.options(csv_options).csv(file_path)

    elif file_format_lower == 'parquet':
        return spark.read.options(**read_options).parquet(file_path)

    elif file_format_lower == 'json':
        if record_path:
            # Nested JSON (e.g. {"result": {"data": [...]}})
            raw_json = spark.read.text(file_path, wholetext=True).first()[0]
            data = json.loads(raw_json)

            # Traverse nested path (e.g. "result.data" → data["result"]["data"])
            nested = data
            for key in record_path.split('.'):
                nested = nested[key]
            rdd = spark.sparkContext.parallelize([json.dumps(r) for r in nested])
            df = spark.read.json(rdd)
        else:
            # Flat JSON array — Spark reads each element as a row
            df = spark.read.json(file_path)

        # Use JsonFileHandler for validation,type conversion, and optional flattening
        if handler:
            preserve = preserve_complex_types if preserve_complex_types is not None else True
            df, report = handler.process_dataframe(
                df,
                flatten=flatten_structs,
                preserve_complex_types=preserve,
                preserve_structs=not flatten_structs,  # Preserve structs as JSON when not flattening
                validate=True,
                convert_types=True,  # Always enforce type conversion for schema consistency
                add_missing=False,
                allow_undefined_columns_as_string=allow_undefined_columns_as_string
            )
            logger.info(f"Processed JSON with JsonFileHandler. Steps: {report['steps_performed']}")
            if 'validation' in report:
                if report['validation'].get('type_mismatches'):
                    logger.warning(f"Type mismatches detected and converted: {report['validation']['type_mismatches']}")
            logger.info(f"Final schema: {[(f[0], f[1]) for f in report['final_schema']]}")

        if 'users' in df.columns:
            df = df.withColumn('users', F.col('users').cast('string'))
        return df

    else:
        supported_formats = ['csv', 'parquet', 'json']
        raise ValueError(f"Unsupported file format '{file_format}'. Supported formats: {supported_formats}")


def add_metadata(df: DataFrame, ingest_date: str, file_path: str, schema_name: str, table_name: str, lineage_id: str) -> DataFrame:
    return (
        df
            .withColumn("_ingest_time", F.current_timestamp())
            .withColumn("_ingest_date", F.lit(ingest_date))
            .withColumn("_source_file", F.lit(file_path))
            .withColumn("_source_system", F.lit(schema_name))
            .withColumn("_table", F.lit(table_name))
            .withColumn("_lineage_id", F.lit(lineage_id))
            #.withColumn("_run_id", F.lit(run_id))
    )


def load_files_to_delta(files_to_process: List[str], lakehouse_path: str, schema_name: str, table_name: str, load_strategy:str, handler: 'JsonFileHandler' = None):
    '''Load all files for a source object into the corresponding bronze table as a single batch'''
    dataframes = []
    for file in files_to_process:
        date_folder = re.search(r"\d{4}-\d{2}-\d{2}", file['file_path'])
        ingest_date = date_folder.group(0) if date_folder else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        lineage_id_match = re.search(r"lineage_id=([0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})/", file['file_path'])
        lineage_id = lineage_id_match.group(1)

        logger.info('Reading files from processing directory to a dataframe')
        df = read_single_file(lakehouse_path, file['file_path'], Path(file['file_path']).suffix[1:], handler=handler)
        df = add_metadata(df, ingest_date, file['file_path'], schema_name, table_name, lineage_id)
        dataframes.append(df)

    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)

    if load_strategy not in ['overwrite','append']:
        logger.exception('Invalid load strategy! Only overwrite or append are allowed')
        raise Exception ('Invalid load strategy! Only overwrite or append are allowed')
            
    logger.info(f"{load_strategy} data to table /Tables/{table_path.split('/Tables/')[1]}")
    (
        combined_df.write
        .format('delta')
        .mode(load_strategy)
        .option("mergeSchema", "true")
        .save(table_path)
    )

    output = {
            'files_processed': len(dataframes),
            'total_records': combined_df.count(),
            'final_columns': combined_df.columns
    }
    return output


def execute_ingestion(files_to_process: List[Dict[str, Any]], lakehouse_path: str, schema_name: str, table_name: str, load_strategy: str, handler: 'JsonFileHandler' = None) -> Dict[str, Any]:
    if not files_to_process:
        logger.info('No files to process.')
        return {'files_processed': 0}
    else:
        return load_files_to_delta(files_to_process, lakehouse_path, schema_name, table_name, load_strategy, handler)


def update_stage_status(status: str, log_run_data: list, metadata_db):
    """Update database records for current and previous stages"""
    
    # Status mappings
    status_config = {
        'Completed': {'current': 'Ready', 'previous': 'Processed'},
        'Failed': {'current': 'N/A', 'previous': 'Failed'},
        'No files to process': {'current': 'N/A', 'previous': 'No files to process'}
    }

    config = status_config.get(status, {'current': 'N/A', 'previous': 'N/A'})

    try:
        records = ast.literal_eval(log_run_data) if isinstance(log_run_data, str) else log_run_data
    except Exception:
        logger.exception('log_run_data is not valid; skipping updates')
        records = []

    for record in records:
        # Update current stage
        try:
            logger.info(f"Updating current stage logs for {record['current_task_executions_id']}")
            metadata_db.execute_stored_procedure(
                'logging.usp_log_task_execution_completion',
                False,
                task_executions_id=record['current_task_executions_id'],
                status=status,
                next_stage_status=config['current']
            )
        except Exception:
            logger.exception(f"Failed to update current stage for {record['current_task_executions_id']}")
        
        # Update previous stage
        try:
            metadata_db.execute_stored_procedure(
                'logging.usp_update_processed_flag', 
                False, 
                task_executions_id=record['previous_task_executions_id'], 
                next_stage_status=config['previous']
            )
        except Exception:
            logger.exception(f"Failed to update previous stage for {record['previous_task_executions_id']}")


def finalize_ingestion(status: str, log_run_data: list, processing_prefix: str, archive_prefix: str, incoming_prefix: str, metadata_db):

    # Update database records
    update_stage_status(status, log_run_data, metadata_db)

    # Handle file cleanup based on status
    if status in ['Completed']:
        try:
            logger.info('Archiving processed files...')
            moved_files = move_between_stages(processing_prefix, archive_prefix)
        except Exception:
            logger.exception('Failed to move processed files to archive')

    elif status == 'Failed':
        logger.info('Load failed; returning files to incoming directory for retry...')
        try:
            moved_files = move_between_stages(processing_prefix, incoming_prefix) # Return unprocessed files to processing folder
        except Exception:
            logger.exception('Failed to return files to incoming directory')

    elif status == 'No files to process':
        pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Main Execution Logic

# CELL ********************

metadata_db = SQLDatabase.from_metadata_control_db(akv_uri, tenant_id_secret_name, client_id_secret_name, client_secret_name, server, database)

current_run_id = notebookutils.runtime.context.get('currentRunId')
run_id = current_run_id or notebookutils.runtime.context.get('activityId')

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
load_strategy = target_settings.get('load_strategy')

option_settings = json.loads(option_settings)
record_path = option_settings.get('record_path')
flatten_structs = option_settings.get('flatten_structs', False)
preserve_complex_types = option_settings.get('preserve_complex_types', True)
allow_undefined_columns_as_string = option_settings.get('allow_undefined_columns_as_string', False)
expected_columns = option_settings.get('expected_columns')

# Initialize JsonFileHandler for schema-aware JSON processing (only when needed)
json_handler = None
if flatten_structs or expected_columns:
    json_handler = JsonFileHandler.from_option_settings(option_settings)
    logger.info(f"Initialized JsonFileHandler with {len(json_handler.expected_schema)} expected columns")

log_run_pairs = ast.literal_eval(log_run_pairs)
log_run_data = [{**item, 'current_task_executions_id': str(uuid.uuid4())} for item in log_run_pairs]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Log start
log_start_parameters = {
    'task_executions_id': '', # will be replaced later when executed
    'lineage_id': '', # will be replaced later when executed
    'run_id': run_id,
    'parent_run_id': parent_run_id,
    'task_id': task_id,
    'workspace_id': notebookutils.runtime.context.get('currentWorkspaceId'),
    'executing_object_id': notebookutils.runtime.context.get('currentNotebookId'),
    'executing_object_name': notebookutils.runtime.context.get('currentNotebookName'),
    'executing_object_run_id': notebookutils.runtime.context.get('activityId'),
    'executing_object_type': 'notebook'
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build paths
lakehouse_path = f'abfss://{bronze_workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}'
incoming_prefix = f'{lakehouse_path}/Files/incoming/{target_file_path}'
processing_prefix = f'{lakehouse_path}/Files/processing/{target_file_path}'
archive_prefix = f'{lakehouse_path}/Files/archive/{target_file_path}'
table_path = f'{lakehouse_path}/Tables/{schema_name}/{table_name}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Begin processing
def process_batch(incoming_prefix, processing_prefix, archive_prefix, lakehouse_path, schema_name, table_name, run_id, log_run_data, metadata_db, log_start_parameters, load_strategy, handler=None):
    status = 'Failed'
    try:
        moved_files = move_between_stages(incoming_prefix, processing_prefix)
        files_to_process = list_files(processing_prefix)
        if not files_to_process:
            status = 'No files to process'
            return {'status': status, 'files': []}

        # Generate a bronze row in the logging table for each row from raw. If two rows of extractions occured in raw since the last bronze run, create a row for each to maintain lineage. 
        for record in log_run_data:
            log_start_parameters['lineage_id'] = record['previous_lineage_id']
            log_start_parameters['task_executions_id'] = record['current_task_executions_id']
            metadata_db.execute_stored_procedure('logging.usp_log_task_execution', False, **log_start_parameters)
        run_metrics = execute_ingestion(files_to_process, lakehouse_path, schema_name, table_name, load_strategy, handler)
        status = 'Completed'
        return {'status': status, 'metrics': run_metrics}
    except Exception:
        logger.exception('Processing error.')
        status = 'Failed'
        raise
    finally:
        try:
            finalize_ingestion(status, log_run_data, processing_prefix, archive_prefix, incoming_prefix, metadata_db)
        except Exception:
            logger.exception('Error finalizing ingestion run.')

results = process_batch(incoming_prefix, processing_prefix, archive_prefix, lakehouse_path, schema_name, table_name, run_id, log_run_data, metadata_db, log_start_parameters, load_strategy, json_handler)
print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_lh_row_count = spark.read.format('delta').load(table_path).count()

metadata_db.execute_stored_procedure(
    'logging.usp_log_update_row_count_audit',
    return_results = False,
    parent_run_id = log_start_parameters['parent_run_id'],
    stage = 'bronze',
    row_count = bronze_lh_row_count,
    task_id = log_start_parameters['task_id'],
    task_executions_id = log_start_parameters['task_executions_id']
)
print("Logged record counts in logging.row_count_audit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
