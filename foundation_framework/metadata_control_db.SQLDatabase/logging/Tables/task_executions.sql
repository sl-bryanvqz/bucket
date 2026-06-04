CREATE TABLE [logging].[task_executions] (
    [task_executions_id] UNIQUEIDENTIFIER NOT NULL,
    [lineage_id]         UNIQUEIDENTIFIER NOT NULL,
    [run_id]             UNIQUEIDENTIFIER NOT NULL,
    [parent_run_id]      UNIQUEIDENTIFIER NULL,
    [task_id]            INT              NOT NULL,
    [task_name]          VARCHAR (200)    NULL,
    [stage]              VARCHAR (50)     NULL,
    [status]             VARCHAR (20)     NULL,
    [num_retries]        INT              NULL,
    [start_date]         DATE             NOT NULL,
    [start_datetime]     DATETIME2 (6)    NOT NULL,
    [end_datetime]       DATETIME2 (6)    NULL,
    [template_name]      VARCHAR (200)    NULL,
    [workspace_id]       UNIQUEIDENTIFIER NULL,
    [log_data]           VARCHAR (8000)   NULL,
    [monitoring_url]     VARCHAR (200)    NULL,
    [next_stage_status]  VARCHAR (20)     NULL,
    [object_name]        VARCHAR (200)    NULL,
    [error_details]      VARCHAR (8000)   NULL
);


GO

