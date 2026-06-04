
CREATE PROCEDURE logging.usp_update_processed_flag
    @task_executions_id UNIQUEIDENTIFIER,
    @next_stage_status VARCHAR(20)
AS
BEGIN
    UPDATE
        logging.task_executions
    SET
        next_stage_status = @next_stage_status
    WHERE
        task_executions_id = @task_executions_id
END

GO

