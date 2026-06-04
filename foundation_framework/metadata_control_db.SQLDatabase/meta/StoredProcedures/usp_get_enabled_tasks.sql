
CREATE PROCEDURE [meta].[usp_get_enabled_tasks]
    @stage VARCHAR(100) = NULL
AS
BEGIN
    IF @stage = '' 
        SET @stage = NULL


    IF @stage IS NULL
        SELECT NEWID() AS task_executions_id, NEWID() AS lineage_id, *
        FROM
            meta.task_metadata
        WHERE
            enabled = 1
    ELSE
        SELECT NEWID() AS task_executions_id, NEWID() AS lineage_id, *
        FROM
            meta.task_metadata
        WHERE
            enabled = 1
            AND UPPER(stage) = UPPER(@stage)
END;

GO

