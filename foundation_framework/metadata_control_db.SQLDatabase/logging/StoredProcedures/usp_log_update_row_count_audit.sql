CREATE PROCEDURE [logging].[usp_log_update_row_count_audit]
    @parent_run_id UNIQUEIDENTIFIER,
    @stage VARCHAR(50),
    @row_count BIGINT,
    @task_id INT,
    @task_executions_id VARCHAR(50)
AS
BEGIN
    DECLARE @select_extract_id_sql NVARCHAR(MAX);
    DECLARE @extract_task_id INT;
    SET @select_extract_id_sql = 'SELECT @extract_task_id = extract_task_id FROM meta.task_dependencies WHERE ' + LOWER(@stage) + '_task_id = ' + CONVERT(NVARCHAR(MAX), @task_id);
    EXEC sp_executesql @select_extract_id_sql, N'@extract_task_id INT OUTPUT', @extract_task_id = @extract_task_id OUTPUT;

    UPDATE logging.row_count_audit
    SET
        bronze_lh_row_count = CASE WHEN UPPER(@stage) = 'BRONZE' THEN @row_count ELSE bronze_lh_row_count END,
        bronze_run_status = CASE WHEN UPPER(@stage) = 'BRONZE' THEN 'COMPLETE' ELSE bronze_run_status END,
        bronze_run_task_executions_id = CASE WHEN UPPER(@stage) = 'BRONZE' THEN @task_executions_id ELSE bronze_run_task_executions_id END,
        bronze_task_id = CASE WHEN UPPER(@stage) = 'BRONZE' THEN @task_id ELSE bronze_task_id END,

        silver_lh_row_count = CASE WHEN UPPER(@stage) = 'SILVER' THEN @row_count ELSE silver_lh_row_count END,
        silver_run_status = CASE WHEN UPPER(@stage) = 'SILVER' THEN 'COMPLETE' ELSE silver_run_status END,
        silver_run_task_executions_id = CASE WHEN UPPER(@stage) = 'SILVER' THEN @task_executions_id ELSE silver_run_task_executions_id END,
        silver_task_id = CASE WHEN UPPER(@stage) = 'SILVER' THEN @task_id ELSE silver_task_id END

    WHERE parent_run_id = @parent_run_id AND extract_task_id = @extract_task_id

END;

GO

