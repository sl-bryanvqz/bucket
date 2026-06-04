# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Packages/Libraries

# CELL ********************

from pyspark.sql import SparkSession, Row, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, ArrayType
)
import json, yaml

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_shared_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## JDBC Connection

# CELL ********************


def get_metadata_control_db_conn_info(_server: str ="NA",schema_name: str = "dbo") -> Dict[str, str]:
    """
    Retrieve metadata control database connection information from Azure Key Vault.
    
    Args:
        schema_name: Default schema name (defaults to 'dbo')
        
    Returns:
        Dictionary with connection parameters
        
    Raises:
        ValueError: If required secrets are missing or empty
    """
    try:
        # Get variable libraries
        vl_authentication = notebookutils.variableLibrary.getLibrary('vl_authentication')
        vl_guids = notebookutils.variableLibrary.getLibrary('vl_guids')

        # Extract configuration values
        akv_uri = vl_authentication.akv_uri
        tenant_id_secret_name = vl_authentication.tenant_id_secret_name
        client_id_secret_name = vl_authentication.client_id_secret_name
        client_secret_name = vl_authentication.client_secret_name

        # Retrieve secrets from Key Vault
        tenant_id = notebookutils.credentials.getSecret(akv_uri, tenant_id_secret_name)
        client_id = notebookutils.credentials.getSecret(akv_uri, client_id_secret_name)
        client_secret = notebookutils.credentials.getSecret(akv_uri, client_secret_name)

        # Validate secrets were retrieved
        if not all([tenant_id, client_id, client_secret]):
            raise ValueError("One or more secrets could not be retrieved from Key Vault")

        # server = vl_guids.metadata_control_db_server
        if _server == "NA":
            server = vl_guids.metadata_control_db_server
        else:
            server = _server
        
        database = vl_guids.metadata_control_db_database

        return {
            'tenant_id': tenant_id,
            'client_id': client_id,
            'client_secret': client_secret,
            'server': server,
            'database': database,
            'schema_name': schema_name
        }
        
    except Exception as e:
        logging.error(f"Failed to retrieve database connection information: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

metadata_config = get_metadata_control_db_conn_info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _test_jdbc_connection(jdbc_url,driver):

    try:
        (spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "(SELECT 'Successfull' AS Result) t")
        .option("driver", driver)
        .load()
        ).show()
    except Exception as e:
        logging.error(f"Failed to retrieve database connection information: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


jdbc_hostname = metadata_config['server']
jdbc_database = metadata_config['database']
jdbc_url = (
  f"jdbc:sqlserver://{jdbc_hostname}:1433;"
  f"database={jdbc_database};"
  f"authentication=ActiveDirectoryServicePrincipal;"
  f"aadSecurePrincipalId={metadata_config['client_id']};"
  f"aadSecurePrincipalSecret={metadata_config['client_secret']};"
  f"aadAuthority=https://login.microsoftonline.com/{metadata_config['tenant_id']}"
)
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# _test_jdbc_connection(jdbc_url,driver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transformation

# MARKDOWN ********************

# ## **Config file Path**

# CELL ********************

# path=f"{notebookutils.nbResPath}/builtin/config.yml"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

def _load_table_db(jdbc_url: str, driver: str, table_name: str):
    df_table = (spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("driver", driver)
        .load()
        )
    return df_table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _define_schema_final_df():
    schema = StructType([
        StructField("task_id", IntegerType(), True),
        StructField("task_name", StringType(), True),
        StructField("object_name", StringType(), True),
        StructField("stage", StringType(), True),
        # StructField("source_connection_id", IntegerType(), True),
        StructField("source_connection_name", StringType(), True),
        StructField("source_settings", StringType(), True),
        StructField("target_settings", StringType(), True),
        StructField("option_settings", StringType(), True),
        StructField("template_settings", StringType(), True),
        StructField("enabled", IntegerType(), True),
        StructField("file_hash", StringType(), True),
        StructField("scheduling_settings", StringType(), True),
        StructField("enable_retries", IntegerType(), True),
        StructField("max_retry_attempts", IntegerType(), True)
    ])
    return schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Utility Functions

# CELL ********************

def _read_yaml_text(path: str) -> str:
    with open(path) as data:
        _config = yaml.safe_load(data)
    return _config

def _norm_stage(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.replace(" ","_")
    return s

def _deduce_stages(tbl: dict) -> list:
    # Prefer explicit list under 'stage'; else infer from keys of source/target settings
    stages = tbl.get("stage")
    if isinstance(stages, str):
        stages = [stages]
    if isinstance(stages, list) and stages:
        return [_norm_stage(s) for s in stages]
    found = []
    for sect in ("source_settings", "target_settings"):
        for item in tbl.get(sect, []) or []:
            for k in item.keys():
                n = _norm_stage(k)
                if n and n not in found:
                    found.append(n)
    return found

def _collect_stage_dict(items: list, stage_name: str) -> dict:
    # Merge dicts that correspond to a stage into one dictionary
    merged = {}
    for item in items or []:
        for k, v in item.items():
            if _norm_stage(k) == stage_name and isinstance(v, dict):
                merged.update(v)
    return merged

def _to_json_or_NULL(obj: dict) -> str:
    if not obj:
        return None
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        # Convert non-serializable values to strings
        return json.dumps({k: str(v) for k, v in obj.items()}, ensure_ascii=False)

def yaml_to_stage_rows(yaml_path: str):
    """
    Reads YAML and returns a DataFrame with one row per stage per table.
"""
    cfg = _read_yaml_text(yaml_path)
    tables = cfg.get("tables") or []

    rows = []
    for tbl in tables:
        object_name = tbl.get("object_name")
        stages = _deduce_stages(tbl)

        # Table-level fields
        enabled_val = 1 if tbl.get("enabled")=="true" else 0
        enable_retries_val = 1 if tbl.get("enable_retries") else 0
        max_retry_attempts_val = int(tbl.get("max_retry_attempts") or 0)


        file_hash_val = tbl.get("file_hash")
        file_hash_val = str(file_hash_val) if file_hash_val not in (None, "", []) else None

        scheduling_json = _to_json_or_NULL(tbl.get("scheduling_settings") or {})
        template_json = _to_json_or_NULL(tbl.get("template_settings") or {})
        # option_json = _to_json_or_NULL(tbl.get("option_settings") or {})

        source_conn_id = tbl.get("source_connection_id")
        try:
            source_conn_id = int(source_conn_id) if source_conn_id is not None else None
        except Exception:
            source_conn_id = None

        # Stage-specific blocks
        src_items = tbl.get("source_settings") or []
        tgt_items = tbl.get("target_settings") or []
        scheduling_items = tbl.get("scheduling_settings") or []
        source_conn_items = tbl.get("source_connections") or []
        template_set_items = tbl.get("template_settings") or []
        option_set_items = tbl.get("option_settings") or []
        # print(f"Source connection :{source_conn_items}")
        for i, stage_name in enumerate(stages):
            src_det = _collect_stage_dict(src_items, stage_name)
            tgt_det = _collect_stage_dict(tgt_items, stage_name)
            scheduling_det = _collect_stage_dict(scheduling_items, stage_name)
            source_connection_det = _collect_stage_dict(source_conn_items, stage_name)
            template_set_det = _collect_stage_dict(template_set_items, stage_name)
            option_set_det =_collect_stage_dict(option_set_items,stage_name)
            # task fields: prefer source side, else target side
            det = dict(src_det)
            for k, v in tgt_det.items():
                det.setdefault(k, v)

            task_name = det.get("task_name")

            # JSON strings for stage-specific settings

            src_det = {key: value for key, value in src_det.items() if key != 'task_name'}
            tgt_det = {key: value for key, value in tgt_det.items() if key != 'task_name'}
            
            src_json = _to_json_or_NULL(src_det)
            tgt_json = _to_json_or_NULL(tgt_det)
            schd_json = _to_json_or_NULL(scheduling_det)
            temp_json = _to_json_or_NULL(template_set_det)
            _option_json =_to_json_or_NULL(option_set_det)
            # Populate Extract-only fields; others are "NULL"
            is_extract = stage_name == "Extract"
            # row_scheduling = schd_json if is_extract else "NULL"
            row_template = template_json if is_extract else None
            # row_source_conn = source_conn_id if is_extract else None
            row_scheduling = schd_json
            row_source_conn = source_conn_id

            # Option settings: if you want "NULL" as in the example, force it.
            # Otherwise set 'option_json' below.
            row_option = None
            rows.append(Row(
                task_id=None,
                task_name=task_name,
                object_name=str(object_name) if object_name is not None else None,
                stage=tbl.get("stage")[i],
                # source_connection_id=row_source_conn,
                source_connection_name=source_connection_det.get("source_connection_name",None),
                source_settings=src_json,
                target_settings=tgt_json,
                option_settings=_option_json,
                template_settings=temp_json,
                enabled=enabled_val,
                file_hash=file_hash_val,
                scheduling_settings=row_scheduling,
                enable_retries=enable_retries_val,
                max_retry_attempts=max_retry_attempts_val
             ))
        print(f'Successfully loaded config to dataframe for object:{object_name}')
    schema = _define_schema_final_df()
    df = spark.createDataFrame(rows, schema=schema)
    # get connection configs
    df_con=_load_table_db(jdbc_url, driver, "meta.connections").select('connection_id','connection_name')
    # get connection id
    df=df.join(df_con,(df.source_connection_name==df_con.connection_name), how="left")
    df=df.withColumnRenamed('connection_id','source_connection_id')
    #sequence dataframe based on target table
    columns_seq = _load_table_db(jdbc_url, driver, "meta.task").columns

    return df.select(columns_seq)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation Functions

# CELL ********************

_allowed_stages = ["Extract", "Load to bronze", "Load to silver","Silver Transform"]
_extract_source_keys = ["schema_name", "table_name", "is_incremental"]
_to_bronze_source_keys = ["target_file_path", "target_file_name", "target_file_type"]
_to_silver_source_keys  = ["schema_name", "table_name"]
_extract_target_keys = ["target_file_path","target_file_name"]
_to_bronze_target_keys = ["schema_name","table_name","load_strategy"]
_to_silver_target_keys = ["schema_name","table_name","load_strategy","primary_keys"]
_valid_scheduled_types = ["interval", "time_of_day", "monthly", "yearly","weekly"]
time_pattern = r'^(?:[01]\d|2[0-3]):[0-5]\d$'  # HH:mm in 24-hour format
time_pattern_arr = r'^(?:[01][0-9]|2[0-3]):[0-5][0-9]$'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_scheduling_rules = [
(
    (F.col("schedule_type").isNull()) | F.col("schedule_type").isin(_valid_scheduled_types),
    f"schedule_type must be one of {_valid_scheduled_types} "
),
(
    (F.col("schedule_type") != "interval") | (F.col("frequency_minutes").isNotNull() & (F.col("frequency_minutes") > 0)),
    "interval: frequency_minutes must be > 0 and not null"
),
(
    (F.col("schedule_type") != "time_of_day") | (
        F.col("scheduled_times").isNotNull() &
        (F.size("scheduled_times") > 0) &
        F.expr(f"forall(scheduled_times, t -> lpad(t, 5, '0') rlike '{time_pattern_arr}')")
    ),
    "time_of_day: scheduled_times must be non-empty array of HH:mm"
),
(
    (F.col("schedule_type") != "monthly") | ( 
        F.coalesce(F.col("day_of_month"),F.lit(0)).cast("int").between(1, 31) &
        F.col("time").isNotNull() &
        F.col("time").rlike(time_pattern)
        # time is a single string in schema, so multiple times (array) are not allowed by design
    ),
    "monthly: day_of_month 1-31 and time HH:mm required (no multiple times)"
),
(
   (F.col("schedule_type") != "yearly") | (
        F.col("day").cast("int").isNotNull() &
        F.col("month").cast("int").between(1, 12) &
        F.to_date(
        F.concat_ws("-", F.lit(F.year(F.current_timestamp())), F.col("month").cast("int"), F.col("day").cast("int")),
        "yyyy-M-d"
        ).isNotNull() &                     # this enforces leap-year rule for Feb 29
        F.col("time").isNotNull() &
        F.col("time").rlike(time_pattern)
    ),
    "yearly: valid year-month-day (incl. leap years) and time HH:mm required"
),
(
   (F.col("schedule_type") != "weekly") | (
        (F.size("days_of_week") > 0) &
        F.expr("forall(days_of_week, x -> x >= 1 AND x <= 7)") &
        F.col("time").isNotNull() &
        F.col("time").rlike(time_pattern)
    ),
    "Weekly: days_of_week (values 1-7) and time HH:mm required"
)
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


new_err = None
def _append_error(df, new_err_col):
    """Append new_err_col text into df.error (pipe-separated)."""
    if "warning" not in df.columns:
        return df.withColumn("warning_code",F.lit("")).withColumn("warning", F.lit(None))

    df = df.withColumn(
        "warning",
        F.when(
            df.warning.isNotNull() & new_err_col.isNotNull(),
            F.concat_ws(" | ", df.warning, new_err_col)
        ).otherwise(
            F.when(new_err_col.isNotNull(), new_err_col).otherwise(df.warning)
        )
    )
    df = df.withColumn(
        "warning_code",
        F.when(
            df.warning!='',
            F.lit("E")
        ).otherwise(
            # F.lit("")
            F.when(new_err_col.isNotNull(), new_err_col).otherwise(df.warning_code)
        )
    )
    return df

def add_stage_validation(df):
    """ Validates the if the stages mentioned in the configs are """
    allowed = _allowed_stages

    new_err = F.when(
        ~F.col("stage").isin(allowed),
        F.format_string(
            "Invalid stage '%s'. Allowed values: %s",
            F.col("stage"),
            F.lit(", ".join(allowed))
        )
    )

    return _append_error(df, new_err)

def _settings_validation(df,field_validating_for,extract_source_keys,to_bronze_source_keys,to_silver_source_keys):
    src = F.col(field_validating_for)

    def j(key):
        return F.get_json_object(src, f"$.{key}")
    
    stage = F.col("stage")
    if field_validating_for == "source_settings":
        is_incr_raw = F.lower(F.trim(j("is_incremental")))
        is_incr_true = is_incr_raw.isin("1", "true", "yes", "y")

    extract = (stage == "Extract")
    bronze  = (stage == "Load to bronze")
    silver = (stage == "Load to silver")

    error_parts = []

    # ---- Extract: required keys ----
    for key in extract_source_keys:
        error_parts.append(
            F.when(
                extract & (j(key).isNull() | (F.trim(j(key)) == "")),
                F.lit(f"Extract: {field_validating_for} .{key} is required")
            ).otherwise(F.lit(None))
        )

    if field_validating_for == "source_settings":
        # ---- Extract: conditional keys when incremental ----
        for key in ["incremental_column", "incremental_col_data_type"]:
            error_parts.append(
                F.when(
                    extract & is_incr_true &
                    (j(key).isNull() | (F.trim(j(key)) == "")),
                    F.lit(
                        f"Extract: {field_validating_for}.{key} is required when "
                        "is_incremental is 1/true"
                    )
                )
            )

    # ---- Load to bronze: required keys in source_settings ----
    for key in to_bronze_source_keys:
        error_parts.append(
            F.when(
                bronze & (j(key).isNull() | (F.trim(j(key)) == "")),
                F.lit(f"Load to bronze: {field_validating_for}.{key} is required")
            )
        )

    # ---- Load to silver: required keys in source_settings ----
    for key in to_silver_source_keys:
        error_parts.append(
            F.when(
                silver & (j(key).isNull() | (F.trim(j(key)) == "")),
                F.lit(f"Load to bronze: {field_validating_for}.{key} is required")
            )
        )

    # concat_ws ignores nulls → only real messages remain
    if len(error_parts)>0:
        new_err = F.concat_ws(" | ", *error_parts)

    return _append_error(df, new_err)

def _source_connection_validation(df):
    new_err = F.when(
        (F.col("stage") == "Extract") &
        (F.col("source_connection_id").isNull() |
         (F.trim(F.col("source_connection_id")) == "")),
        F.lit("Extract: source_connection_id is required")
    ).otherwise(F.lit(None))

    return _append_error(df, new_err)

def _validate_scheduling(df, col="scheduling_settings"):
    #define structure for the values
    scheduling_schema = StructType([
    StructField("schedule_type",      StringType()),
    StructField("frequency_minutes",  IntegerType()),
    StructField("scheduled_times",    ArrayType(StringType())),
    StructField("day_of_month",       StringType()),
    StructField("day",                StringType()),
    StructField("month",              StringType()),
    StructField("time",               StringType()),
    StructField("days_of_week",       ArrayType(IntegerType()))    
    ])  
    
    df1 = df.withColumn("sched", F.from_json(F.col(col), scheduling_schema)) \
                .select("*", "sched.*").drop("sched")

    for cond,message in _scheduling_rules:
        new_err = F.when(
                    ~cond,
                    F.lit(message)
                ).otherwise(F.lit(None))
            
        df1 = _append_error(df1, new_err )
    return df1

def final_validation(df):
    df_new = _settings_validation(df,"source_settings",_extract_source_keys,_to_bronze_source_keys,_to_silver_source_keys)
    df_new = _settings_validation(df_new,"target_settings",_extract_target_keys,_to_bronze_target_keys,_to_silver_target_keys)
    df_new = _source_connection_validation(df_new)
    df_new = _validate_scheduling(df_new)
    return df_new.filter("warning_code=='E'").drop("warning_code")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# df_current = yaml_to_stage_rows(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_warning=final_validation(df_current)
# warning_records = df_warning.count()

# if warning_records>0:
#     print(f'WARNING.. Issues with {warning_records} records.')
#     display(df_warning)
# else:
#     print('All records passed validations.')
#     display(df_current)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

def _load_staging_table(df_current):
    df_current=df_current.drop('task_id')
    writer =(
        df_current.write.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "meta.temp_task")
    .option("driver", driver).mode("overwrite")
    )
    writer.save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Upserts

# CELL ********************

def build_merge_sql(
    target_schema: str,
    target_table: str,
    staging_schema: str,
    staging_table: str,
    keys: List[str],
    update_cols: List[str],
    insert_cols: List[str],
) -> str:
    """
    Build a SQL Server MERGE statement:
      MERGE target t
      USING staging s
        ON t.key1 = s.key1 AND ...
      WHEN MATCHED THEN UPDATE SET t.col = s.col, ...
      WHEN NOT MATCHED THEN INSERT (cols...) VALUES (s.cols...)
    """
    t_full = f"{target_schema}.{target_table}"
    s_full = f"{staging_schema}.{staging_table}"


    on_clause = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    set_clause = ",\n        ".join([f"t.{c} = s.{c}" for c in update_cols])

    insert_cols_list = ", ".join([c for c in insert_cols])
    insert_vals_list = ", ".join([f"s.{c}" for c in insert_cols])

    sql = f"""
MERGE {t_full} AS t
USING {s_full} AS s
    ON {on_clause}
WHEN MATCHED THEN
    UPDATE SET
        {set_clause}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insert_cols_list})
    VALUES ({insert_vals_list});
"""
    return sql.strip()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_changes_to_metadatadb(df_current):
    col_list = ['task_name','object_name','stage','source_connection_id','source_settings','target_settings','option_settings','template_settings','enabled','file_hash','scheduling_settings','enable_retries','max_retry_attempts']
    merge_query = build_merge_sql('meta','task','meta','temp_task',['object_name','stage'],col_list,col_list)

    # Extract configuration values
    vl_authentication = notebookutils.variableLibrary.getLibrary('vl_authentication')
    client_secret_name = vl_authentication.client_secret_name
    metadata_db = SQLDatabase.from_metadata_control_db(vl_authentication.akv_uri, vl_authentication.tenant_id_secret_name, vl_authentication.client_id_secret_name, vl_authentication.client_secret_name, metadata_config['server'], metadata_config['database'], schema_name='meta')
    # Load change to staging table
    _load_staging_table(df_current)
    # load changes to meta.task table
    rows = metadata_db.execute_non_query(merge_query)

    print(f"MERGED {rows} rows successfully into meta.task table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
