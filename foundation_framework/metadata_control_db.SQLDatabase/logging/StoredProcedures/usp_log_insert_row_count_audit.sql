
CREATE PROCEDURE [logging].[usp_log_insert_row_count_audit]
    @task_id INT,
    @source_system VARCHAR(100),
    @source_table_name VARCHAR(100),
    @source_row_count BIGINT,
    @is_incremental INT,
    @parent_run_id UNIQUEIDENTIFIER
AS
BEGIN

    INSERT INTO logging.row_count_audit (
        [extract_task_id]
        ,[bronze_task_id]
        ,[silver_task_id]
        ,[source_system]
        ,[source_table]
        ,[source_row_count]
        ,[bronze_lh_row_count]
        ,[silver_lh_row_count]
        ,[is_incremental]
        ,[parent_run_id]
        ,[bronze_run_status]
        ,[bronze_run_task_executions_id]
        ,[silver_run_status]
        ,[silver_run_task_executions_id]
        ,[ingestion_timestamp]
    )
    VALUES(
        @task_id
        , NULL
        , NULL
        , @source_system
        , @source_table_name
        , @source_row_count
        , NULL
        , NULL
        , @is_incremental
        , @parent_run_id
        , 'NOT STARTED'
        , NULL
        , 'NOT STARTED'
        , NULL
        , GETDATE()
    )

END;

GO

