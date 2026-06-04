# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ##### Parameters

# PARAMETERS CELL ********************

task_id = 50
task_name = 'Load to silver - AW Address'
source_settings = '{"schema_name": "sqldb_adventureworks", "table_name": "SalesLT__Address"}'
log_run_pairs = "[{'previous_task_executions_id': '1D993FD2-B0A7-4368-8640-AB136A515907', 'previous_lineage_id': 'E149B7C3-0410-479C-924D-72969FB58F15'}]"
target_settings = '{"schema_name": "sqldb_adventureworks", "table_name": "sales_lt__Address", "load_strategy": "overwrite", "primary_keys": ["AddressID"]}'
option_settings = None
limit_rows_for_debugging = False
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

import ast
import uuid
import json
from delta.tables import DeltaTable
import re
import pyspark.sql.functions as F # import when, col, lit, to_date, to_timestamp, trim
import datetime

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
logger = logging.getLogger('LoadToSilver')
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Function definitions

# CELL ********************

# Define functions for data transformation
# Convert a string to snake_case
def to_snake_case(string):
    string = re.sub(r'([a-z])([A-Z0-9])', r'\1_\2', string)
    string = re.sub(r'([A-Z])([A-Z][a-z])', r'\1_\2', string)
    string = string.lower().strip()
    return string

# Remove leading/trailing spaces from all string column values
def trim_all_string_columns(df):
    string_columns = [field.name for field in df.schema.fields if field.dataType.typeName() == 'string']
    df = df.select(
        *[F.trim(F.col(c)).alias(c) if c in string_columns else F.col(c) for c in df.columns]
    )
    return df

# Replace old dates
def replace_old_dates(df):
    date_columns = [field.name for field in df.schema.fields if field.dataType.typeName() in('date','timestamp')]
    for date_column in date_columns:
        df = df.withColumn(
            date_column, 
		    F.when(
			    F.col(date_column) <= '1900-01-01', F.to_date(F.lit('1900-01-01'), 'yyyy-MM-dd')
            )
			.otherwise(
                F.col(date_column)
            )
        )
    return df

# Define parent function that applies all data transformation functions above
def clean_bronze_table(df, limit_rows_for_debugging=False):
    if limit_rows_for_debugging:
        df = df.limit(1000)

    snake_case_column_names = [to_snake_case(col) for col in df.columns]
    df = df.toDF(*snake_case_column_names)

    #df = df.withColumn("ingested_date_time", (F.col("ingested_date_time").cast("timestamp")))

    df = replace_old_dates(df)

    df = trim_all_string_columns(df)

    return df


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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Main execution

# CELL ********************

metadata_db = SQLDatabase.from_metadata_control_db(akv_uri, tenant_id_secret_name, client_id_secret_name, client_secret_name, server, database)

# Set variables
source_settings = source_settings or '{}'
target_settings = target_settings or '{}'

source_settings = json.loads(source_settings)
source_schema_name = source_settings.get('schema_name')
source_table_name = source_settings.get('table_name')

target_settings = json.loads(target_settings)
target_schema_name = target_settings.get('schema_name')
target_table_name = target_settings.get('table_name')
target_load_strategy = target_settings.get('load_strategy')
target_primary_keys = target_settings.get('primary_keys')

source_lakehouse_name = 'bronze_lh'
target_lakehouse_name = 'silver_lh'

lineage_ids = [record['previous_lineage_id'] for record in ast.literal_eval(log_run_pairs)]

current_run_id = notebookutils.runtime.context.get('currentRunId')
run_id = current_run_id or notebookutils.runtime.context.get('activityId')

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
    'executing_object_type': 'Notebook'
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for record in log_run_data:
    log_start_parameters['lineage_id'] = record['previous_lineage_id']
    log_start_parameters['task_executions_id'] = record['current_task_executions_id']
    metadata_db.execute_stored_procedure('logging.usp_log_task_execution', False, **log_start_parameters)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_lakehouse_path = notebookutils.lakehouse.get(source_lakehouse_name).properties['abfsPath']
source_table_path = f'{source_lakehouse_path}/Tables/{source_schema_name}/{source_table_name}'

target_lakehouse_path = notebookutils.lakehouse.get(target_lakehouse_name).properties['abfsPath']
target_table_path = f'{target_lakehouse_path}/Tables/{target_schema_name}/{target_table_name}'

print(f'Source table path: {source_table_path}')
print('----------')
print(f'Delta table path : {target_table_path}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

status = "Failed"
try:
    sqlContext.read.format('delta').load(source_table_path).createOrReplaceTempView(f'vw_source_{source_table_name}')

    # Get only run_ids that need to be processed
    # Need to create hash, depending on load type overwrite or merges
    filter_condition = "', '".join(lineage_ids)
    primary_keys = ", ".join(target_primary_keys)

    if option_settings and 'enforce_schema' in option_settings:
        options = json.loads(option_settings)
        casted_select_str = ""
        column_list = sqlContext.read.format('delta').load(source_table_path).columns
        for col in column_list:
            if col not in options['enforce_schema']:
                casted_select_str += col + ","
            else:
                casted_select_str += f"CAST({col} as {options['enforce_schema'][col]}) as {col},"

        sql = f'''
        SELECT {casted_select_str} ROW_NUMBER() OVER(PARTITION BY {primary_keys} ORDER BY _ingest_time DESC) AS row_num
        FROM vw_source_{source_table_name}
        WHERE UPPER(_lineage_id) IN('{filter_condition}')
        '''
        logger.info(f'Option to enforce schema found - generating SELECT query with user-defined CAST instructions: {sql}')
    
    else:
        sql = f'''
        SELECT *, ROW_NUMBER() OVER(PARTITION BY {primary_keys} ORDER BY _ingest_time DESC) AS row_num
        FROM vw_source_{source_table_name}
        WHERE UPPER(_lineage_id) IN('{filter_condition}')
        '''
        logger.info(f'Generating catch-all SELECT * query: {sql}')


    # Clean up SOURCE table and add audit columns
    logger.info(f'Beginning execution - reading data from bronze source. Cleaning the table and adding audit columns')
    source_df = spark.sql(sql)
    source_df_deduped = source_df.filter(source_df.row_num == 1).drop('row_num')
    source_df_cleaned = clean_bronze_table(source_df_deduped)
    source_df_cleaned = source_df_cleaned.withColumn('_effective_start_datetime', F.lit(datetime.datetime(1900,1,1,00,00,00)))
    source_df_cleaned = source_df_cleaned.withColumn('_effective_end_datetime', F.lit(datetime.datetime(9999,12,31,23,59,59)))
    source_df_cleaned = source_df_cleaned.withColumn('_current_flag', F.lit(True))

    # Add Hash column for Primary Key
    primary_keys_snake_case = [to_snake_case(target_primary_key) for target_primary_key in target_primary_keys]
    source_df_cleaned = source_df_cleaned.withColumn('hashed_pk', F.sha2(F.concat_ws('||', *primary_keys_snake_case), 256))

    # Add Hash column for all non-primary key columns
    audit_columns = ['_ingest_time','_ingest_date','_source_file','_source_system','_table','_effective_start_datetime','_effective_end_datetime','_lineage_id','_current_flag', 'hashed_pk']
    non_key_remove_columns = primary_keys_snake_case + audit_columns
    non_key_columns = [column for column in source_df_cleaned.columns if column not in non_key_remove_columns]
    source_df_cleaned = source_df_cleaned.withColumn('hashed_row', F.sha2(F.concat_ws('||', *non_key_columns), 256))

    # Create dictionary used for UPDATE in MERGE
    set_column_list = [column for column in non_key_columns+audit_columns if column not in ['_ingest_date','_ingest_time','hashed_pk']] + ['hashed_row']
    set_column_query = {}
    for x in set_column_list:
        if x=='_effective_start_datetime':
            set_column_query["t._effective_start_datetime"] = f"'{datetime.datetime.now().isoformat()}'"
        else:
            set_column_query[f"t.{x}"] = f"s.{x}"
    

    #Check if Target exist, if exists read the original data if not create table and exit
    if not DeltaTable.isDeltaTable(spark, target_table_path):
        logger.info(f'Writing new table to {target_table_path}')
        source_df_cleaned.write.format('delta').mode("overwrite").save(target_table_path)

    else:
        match target_load_strategy:
            case 'merge':
                    # Join the df_new_data dataframe with the existing delta table using the join expression created above, updating all columns with the keys match and inserting new ones
                    # What about deletes
                    logger.info(f'Merging new records into {target_table_path}')
                    delta_table = DeltaTable.forPath(spark, target_table_path)
                    (
                        delta_table.alias("t").merge(
                            source_df_cleaned.alias("s"),
                            't.hashed_pk = s.hashed_pk'
                        ).whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )

            case 'SCD1':
                    logger.info(f'Merging via SCD1 into {target_table_path}')
                    delta_table = DeltaTable.forPath(spark, target_table_path)
                    (
                        delta_table.alias("t").merge(
                            source_df_cleaned.alias("s"),
                            't.hashed_pk = s.hashed_pk'
                        ).whenMatchedUpdate(
                            condition = "(t.hashed_row != s.hashed_row) and (t._current_flag = True)",
                            set = set_column_query
                        )
                        .whenNotMatchedInsertAll()
                        .whenNotMatchedBySourceUpdate(
                            condition = "t._current_flag = True",
                            set = {"t._current_flag": "False",
                                   "t._effective_end_datetime": f"""'{datetime.datetime.now().isoformat()}'"""
                                   }
                        )
                        .execute()
                    )

            case 'SCD2-FULL':
                execution_datetime = datetime.datetime.now().isoformat()
                logger.info(f'Merging via SCD2 into {target_table_path}')

                # Step 1: Identify rows that have changed
                target_df = spark.read.format('delta').load(target_table_path)
                temp_df = source_df_cleaned.alias("source")\
                                .join(target_df.filter("_current_flag = true").alias("target"), ["hashed_pk"], how="left") \
                                .select("source.*", F.coalesce(F.col("target.hashed_row"), F.lit(None)).alias("target_hashed_row")) \
                                 .withColumn("_scd_status", F.when(F.coalesce("source.hashed_row", F.lit(1)) != F.coalesce("target_hashed_row", F.lit(1)), F.lit('NEWRECORD')).otherwise(F.lit('UNCHANGED')))\
                                .select(["_scd_status","source.*"])
                                

                new_rows_to_update_df = temp_df.alias("source")\
                                        .join(target_df.alias("target"), F.expr("target.hashed_pk = source.hashed_pk"), how="inner")\
                                        .where("target._current_flag=True and source.hashed_row <> target.hashed_row")\
                                        .select("source.*")

                logger.info(f'Found {new_rows_to_update_df.count()} updated records')

                # Step 2: Create staged updates dataframe
                    # is_insert is used to determine the SCD status. 
                    # UNCHANGED --> no changes to be made via SCD
                    # NEW RECORD --> includes both existing records that have changed, and also brand new records
                    # DEACTIVATE --> existing records that have changed, to set the record in target table to _current_flag = False

                staged_updates = (\
                    new_rows_to_update_df\
                    .withColumn("_scd_status", F.lit("DEACTIVATE"))\
                    .select(
                        F.col("_scd_status"),
                        *[F.col(c) for c in target_df.columns]
                    )\
                    .union(
                        temp_df
                        .select(
                            F.col("_scd_status"),
                            *[F.col(c) for c in target_df.columns]
                        )
                    )
                )
                staged_updates = staged_updates.drop('_effective_start_datetime')
                staged_updates = staged_updates.withColumn('_effective_start_datetime', F.lit(execution_datetime))
                

                # Step 3. Perform MERGE
                merge_condition = "t.hashed_pk = s.hashed_pk AND s._scd_status IN ('DEACTIVATE','UNCHANGED')"
                insert_values = {col_name: f"s.{col_name}" for col_name in target_df.columns}

                delta_table = DeltaTable.forPath(spark, target_table_path)
                (
                    delta_table.alias("t").merge(
                        staged_updates.alias("s"),
                        merge_condition
                    ).whenMatchedUpdate(
                        condition = "t._current_flag = True and s.hashed_row <> t.hashed_row and s._scd_status = 'DEACTIVATE'",
                        set = {"t._current_flag": "False",
                               "t._effective_end_datetime": f"""'{execution_datetime}'"""
                              }
                    ).whenNotMatchedInsert(
                        condition = "s._scd_status <> 'UNCHANGED'",
                        values = insert_values
                    )
                    .whenNotMatchedBySourceUpdate(
                        condition = "t._current_flag = True",
                        set = {"t._current_flag": "False", "t._effective_end_datetime": f"""'{execution_datetime}'"""}
                    )
                    .execute()
                )
                print(f"SCD2 merge completed successfully for {target_table_path}")



            case 'SCD2-INCREMENTAL':
                execution_datetime = datetime.datetime.now().isoformat()
                logger.info(f'Merging via SCD2 into {target_table_path}')

                # Step 1: Identify rows that have changed
                target_df = spark.read.format('delta').load(target_table_path)
                temp_df = source_df_cleaned.alias("source")\
                                .join(target_df.filter("_current_flag = true").alias("target"), ["hashed_pk"], how="left") \
                                .select("source.*", F.coalesce(F.col("target.hashed_row"), F.lit(None)).alias("target_hashed_row")) \
                                .filter(F.coalesce("source.hashed_row", F.lit(1)) != F.coalesce("target_hashed_row", F.lit(1))) \
                                .select(["source.*"])
                                
                new_rows_to_update_df = temp_df.alias("source")\
                                        .join(target_df.alias("target"), F.expr("target.hashed_pk = source.hashed_pk"), how="inner")\
                                        .where("target._current_flag=True and source.hashed_row <> target.hashed_row")\
                                        .select("source.*")

                logger.info(f'Found {new_rows_to_update_df.count()} updated records')

                # Step 2: Create staged updates dataframe
                staged_updates = (\
                    new_rows_to_update_df\
                    .withColumn("_scd_status", F.lit("Y"))\
                    .select(
                        F.col("_scd_status"),
                        *[F.col(c) for c in target_df.columns]
                    )\
                    .union(
                        temp_df
                        .withColumn("_scd_status", F.lit("N"))
                        .select(
                            F.col("_scd_status"),
                            *[F.col(c) for c in target_df.columns]
                        )
                    )
                )
                staged_updates = staged_updates.drop('_effective_start_datetime')
                staged_updates = staged_updates.withColumn('_effective_start_datetime', F.lit(execution_datetime))

                # Step 3. Perform MERGE
                merge_condition = "t.hashed_pk = s.hashed_pk AND s._scd_status='Y'"
                insert_values = {col_name: f"s.{col_name}" for col_name in target_df.columns}

                delta_table = DeltaTable.forPath(spark, target_table_path)
                (
                    delta_table.alias("t").merge(
                        staged_updates.alias("s"),
                        merge_condition
                    ).whenMatchedUpdate(
                        condition = "t._current_flag = True",
                        set = {"t._current_flag": "False",
                               "t._effective_end_datetime": f"""'{execution_datetime}'"""
                              }
                    ).whenNotMatchedInsert(
                        values = insert_values
                    )
                    .execute()
                )
                print(f"SCD2 merge completed successfully for {target_table_path}")



            # If target_delta_table_load_strategy = 'overwrite' or 'append'
            case _:
                logger.info(f'Writing new records to {target_table_path} via {target_load_strategy}')
                (
                    source_df_cleaned
                    .write.mode(target_load_strategy)
                    .format("delta")
                    .option("mergeSchema", "true")
                    .save(target_table_path)
                )

    status = "Completed"

except Exception:
    logger.exception('Processing error.')
    status = 'Failed'
    raise

finally:
    update_stage_status(status, log_run_data, metadata_db)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_lh_row_count = spark.read.format('delta').load(target_table_path).where(F.col("_current_flag")==True).count()

metadata_db.execute_stored_procedure(
    'logging.usp_log_update_row_count_audit',
    return_results = False,
    parent_run_id = log_start_parameters['parent_run_id'],
    stage = 'silver',
    row_count = silver_lh_row_count,
    task_id = log_start_parameters['task_id'],
    task_executions_id = log_start_parameters['task_executions_id']
)
print("Logged record counts in logging.row_count_audit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gather various metrics about the load/insert operation from the delta history
delta_table = DeltaTable.forPath(spark, target_table_path)
history = delta_table.history(1).select("operationMetrics")
operation_metrics = history.collect()[0]["operationMetrics"]
rows_read = source_df_cleaned.count()
rows_inserted = int(operation_metrics["numOutputRows"])
rows_updated = 0
rows_copied = rows_inserted + rows_updated

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Package the metrics about the load/insert operation as a dictionary and output the result for consumption by the pipeline that invoked the notebook.
result = {
    "rows_inserted": rows_inserted,
    "rows_updated": rows_updated,
    "rows_read": rows_read,
    "rows_copied": rows_copied
}
notebookutils.notebook.exit(result)

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
