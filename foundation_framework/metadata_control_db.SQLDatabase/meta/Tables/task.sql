CREATE TABLE [meta].[task] (
    [task_id]              INT            IDENTITY (1, 1) NOT NULL,
    [task_name]            VARCHAR (200)  NULL,
    [object_name]          VARCHAR (200)  NULL,
    [stage]                VARCHAR (50)   NULL,
    [source_connection_id] INT            NULL,
    [source_settings]      VARCHAR (8000) NULL,
    [target_settings]      VARCHAR (8000) NULL,
    [option_settings]      VARCHAR (MAX)  NULL,
    [template_settings]    VARCHAR (8000) NULL,
    [enabled]              INT            NULL,
    [file_hash]            VARCHAR (8000) NULL,
    [scheduling_settings]  VARCHAR (8000) NULL,
    [enable_retries]       BIT            NULL,
    [max_retry_attempts]   INT            NULL
);


GO

