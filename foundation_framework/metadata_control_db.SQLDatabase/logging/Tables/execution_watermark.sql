CREATE TABLE [logging].[execution_watermark] (
    [task_id]             INT              NOT NULL,
    [source_table]        VARCHAR (200)    NOT NULL,
    [incremental_column]  VARCHAR (200)    NOT NULL,
    [watermark_value]     VARCHAR (200)    NULL,
    [parent_run_id]       UNIQUEIDENTIFIER NULL,
    [ingestion_timestamp] DATETIME2 (6)    NOT NULL
);


GO

