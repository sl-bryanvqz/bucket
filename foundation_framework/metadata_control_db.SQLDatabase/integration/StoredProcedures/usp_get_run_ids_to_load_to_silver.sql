
CREATE PROCEDURE [integration].[usp_get_run_ids_to_load_to_silver]
    @parent_run_id VARCHAR(200)
AS
SELECT
    UPPER(execution.task_executions_id) AS previous_task_executions_id,
    UPPER(execution.lineage_id) AS previous_lineage_id,
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
WHERE
    meta.stage = 'Load to silver'
    AND execution.stage = 'Load to bronze'
    AND execution.next_stage_status = 'Ready'
    AND execution.status = 'Completed'
    AND execution.parent_run_id = @parent_run_id
    AND meta.enabled = 1

GO

