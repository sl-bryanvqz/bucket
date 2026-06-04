
CREATE PROCEDURE [logging].[usp_log_task_execution]
    @task_executions_id UNIQUEIDENTIFIER,
    @lineage_id UNIQUEIDENTIFIER,
    @run_id UNIQUEIDENTIFIER,
    @parent_run_id UNIQUEIDENTIFIER,
    @task_id INT,
    @workspace_id UNIQUEIDENTIFIER,
    @executing_object_id UNIQUEIDENTIFIER,
    @executing_object_name VARCHAR(200),
    @executing_object_run_id UNIQUEIDENTIFIER,
    @executing_object_type VARCHAR(20)
AS
BEGIN
    SET NOCOUNT OFF

    DECLARE
        @task_name VARCHAR(200),
        @stage VARCHAR(50),
        @template_name VARCHAR(100),
        @monitoring_url VARCHAR(200),
        @object_name VARCHAR(200)

    SELECT
        @task_name = task_name,
        @stage = stage,
        @template_name = COALESCE(JSON_VALUE(template_settings, '$.template_name'), @executing_object_name),
        @monitoring_url =
            CASE
                WHEN LOWER(@executing_object_type) = 'pipeline' THEN LOWER('https://app.powerbi.com/workloads/data-pipeline/monitoring/workspaces/' + CONVERT(varchar(36), @workspace_id) + '/pipelines/' + @executing_object_name + '/' + CONVERT(VARCHAR(36), @executing_object_run_id))
                WHEN LOWER(@executing_object_type) = 'notebook' THEN LOWER('https://app.powerbi.com/workloads/de-ds/sparkmonitor/' + CONVERT(VARCHAR(36), @executing_object_id) + '/' + CONVERT(VARCHAR(36), @executing_object_run_id) + '/?experience=fabric-developer/')
                ELSE NULL
            END,
        @object_name = object_name
    FROM
        meta.task
    WHERE
        task_id = @task_id

    INSERT INTO logging.task_executions (
        task_executions_id,
        lineage_id,
        run_id,
        parent_run_id,
        task_id,
        task_name,
        stage,
        start_date,
        start_datetime,
        template_name,
        workspace_id,
        monitoring_url,
        object_name
    )
    VALUES (
        @task_executions_id,
        @lineage_id,
        @run_id,
        @parent_run_id,
        @task_id,
        @task_name,
        @stage,
        CONVERT(DATE, GETDATE()),
        GETDATE(),
        @template_name,
        @workspace_id,
        @monitoring_url,
        @object_name
    )
END

GO

