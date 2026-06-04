CREATE PROCEDURE [logging].[usp_update_watermark_val]
    @task_id VARCHAR(50)
    , @source_table varchar(200)
    , @incremental_column varchar(200)
    , @watermark_value varchar(200)
    , @parent_run_id varchar(200)
AS
BEGIN

    MERGE INTO logging.execution_watermark AS target
    USING (
    SELECT 
        @task_id AS task_id,
        @source_table AS source_table,
        @incremental_column AS incremental_column,
        @watermark_value AS watermark_value,
        @parent_run_id AS parent_run_id,
        GETDATE() as ingestion_timestamp
    )
    AS SOURCE
    ON target.task_id = source.task_id
    AND target.parent_run_id = source.parent_run_id
    WHEN NOT MATCHED THEN
        INSERT (task_id, source_table, incremental_column, watermark_value, parent_run_id, ingestion_timestamp)
        VALUES (source.task_id, source.source_table, source.incremental_column, source.watermark_value, source.parent_run_id, ingestion_timestamp)
    WHEN MATCHED THEN
        UPDATE SET 
            target.source_table = source.source_table,
            target.incremental_column = source.incremental_column,
            target.watermark_value = source.watermark_value,
            target.parent_run_id = source.parent_run_id;
            

END

GO

