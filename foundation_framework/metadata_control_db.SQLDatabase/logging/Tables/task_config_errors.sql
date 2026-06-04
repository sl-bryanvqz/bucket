CREATE TABLE [logging].[task_config_errors] (
    [task_config_error_id]         UNIQUEIDENTIFIER DEFAULT (newid()) NOT NULL,
    [task_id]                      INT              NOT NULL,
    [detected_at]                  DATETIME2 (3)    DEFAULT (getutcdate()) NOT NULL,
    [error_code]                   VARCHAR (100)    NOT NULL,
    [error_message]                NVARCHAR (4000)  NOT NULL,
    [scheduling_settings_snapshot] NVARCHAR (MAX)   NULL,
    [validator_version]            VARCHAR (20)     DEFAULT ('v1') NOT NULL,
    CONSTRAINT [PK_task_config_errors] PRIMARY KEY CLUSTERED ([task_config_error_id] ASC)
);


GO

