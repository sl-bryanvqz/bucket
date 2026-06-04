
CREATE PROCEDURE logging.usp_log_task_execution_completion
    @task_executions_id UNIQUEIDENTIFIER,
    @status VARCHAR(50),
    @next_stage_status VARCHAR(20)
AS
BEGIN
    UPDATE
        logging.task_executions
    SET
        status = @status,
        next_stage_status = @next_stage_status,
        end_datetime = GETDATE()
    WHERE
        task_executions_id = @task_executions_id
        AND end_datetime IS NULL
END

GO

