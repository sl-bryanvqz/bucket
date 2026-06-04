
CREATE PROCEDURE [integration].[usp_get_files_to_load_to_bronze]
    @bronze_workspace_id VARCHAR(200),
    @bronze_lakehouse_id VARCHAR(200),
    @parent_run_id VARCHAR(200)
AS
SELECT
    --execution.task_log_id AS raw_task_log_id,
    --execution.lineage_id AS raw_lineage_id,
    UPPER(execution.task_executions_id) AS previous_task_executions_id,
    UPPER(execution.lineage_id) AS previous_lineage_id,
    @bronze_workspace_id AS bronze_workspace_id,
    @bronze_lakehouse_id AS bronze_lakehouse_id,
    meta.task_id,
    meta.task_name,
    meta.source_settings,
    meta.target_settings,
    meta.option_settings
FROM
    meta.task AS meta
INNER JOIN
    logging.task_executions AS execution
    ON
        execution.object_name = meta.object_name
        AND execution.stage = 'Extract'
WHERE
    meta.stage = 'Load to bronze'
    AND execution.next_stage_status = 'Ready'
    AND execution.status = 'Completed'
    AND execution.parent_run_id = @parent_run_id
    AND meta.enabled = 1

GO

