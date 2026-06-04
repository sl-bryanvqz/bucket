CREATE TABLE [meta].[temp_task] (
    [task_name]            NVARCHAR (MAX) NULL,
    [object_name]          NVARCHAR (MAX) NULL,
    [stage]                NVARCHAR (MAX) NULL,
    [source_connection_id] INT            NULL,
    [source_settings]      NVARCHAR (MAX) NULL,
    [target_settings]      NVARCHAR (MAX) NULL,
    [option_settings]      NVARCHAR (MAX) NULL,
    [template_settings]    NVARCHAR (MAX) NULL,
    [enabled]              INT            NULL,
    [file_hash]            NVARCHAR (MAX) NULL,
    [scheduling_settings]  NVARCHAR (MAX) NULL,
    [enable_retries]       INT            NULL,
    [max_retry_attempts]   INT            NULL
);


GO

