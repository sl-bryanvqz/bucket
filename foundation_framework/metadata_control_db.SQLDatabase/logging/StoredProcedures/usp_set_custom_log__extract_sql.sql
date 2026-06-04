
CREATE PROCEDURE logging.usp_set_custom_log__extract_sql
    @task_executions_id UNIQUEIDENTIFIER,
    @source_fabric_connection_id VARCHAR(200),
    @source_database_name VARCHAR(200),
    @source_schema_name VARCHAR(200),
    @source_table_name VARCHAR(200),
    @target_lakehouse_id VARCHAR(200),
    @target_workspace_id VARCHAR(200),
    @target_file_path VARCHAR(400),
    @target_file_name VARCHAR(200),
    @sql_query VARCHAR(400)
AS
BEGIN
    DECLARE @log_data VARCHAR(8000)
    SELECT @log_data = JSON_OBJECT(
                    'source_fabric_connection_id': @source_fabric_connection_id,
                    'source_database_name': @source_database_name,
                    'source_schema_name': @source_schema_name,
                    'source_table_name': @source_table_name,
                    'target_lakehouse_id': @target_lakehouse_id,
                    'target_workspace_id': @target_workspace_id,
                    'target_file_path': @target_file_path,
                    'target_file_name': @target_file_name,
                    'sql_query': @sql_query
                )

    UPDATE
        logging.task_executions
    SET
        log_data = @log_data 
    WHERE
        task_executions_id = @task_executions_id
        AND log_data IS NULL
END;

GO

