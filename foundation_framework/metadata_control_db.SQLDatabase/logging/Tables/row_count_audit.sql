CREATE TABLE [logging].[row_count_audit] (
    [extract_task_id]               INT              NOT NULL,
    [bronze_task_id]                INT              NULL,
    [silver_task_id]                INT              NULL,
    [source_system]                 VARCHAR (200)    NULL,
    [source_table]                  VARCHAR (200)    NULL,
    [source_row_count]              BIGINT           NULL,
    [bronze_lh_row_count]           BIGINT           NULL,
    [silver_lh_row_count]           BIGINT           NULL,
    [is_incremental]                VARCHAR (50)     NULL,
    [parent_run_id]                 UNIQUEIDENTIFIER NULL,
    [bronze_run_status]             VARCHAR (50)     NULL,
    [bronze_run_task_executions_id] VARCHAR (50)     NULL,
    [silver_run_status]             VARCHAR (50)     NULL,
    [silver_run_task_executions_id] VARCHAR (50)     NULL,
    [ingestion_timestamp]           DATETIME2 (6)    NULL
);


GO

