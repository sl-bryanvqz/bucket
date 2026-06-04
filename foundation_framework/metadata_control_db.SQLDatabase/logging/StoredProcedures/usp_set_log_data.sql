
CREATE PROCEDURE [logging].[usp_set_log_data]
    @task_executions_id UNIQUEIDENTIFIER,
    @log_data_string VARCHAR(MAX)
AS
BEGIN

    UPDATE
        logging.task_executions
    SET
        log_data = @log_data_string
    WHERE
        task_executions_id = @task_executions_id
        AND log_data IS NULL
END;

GO

