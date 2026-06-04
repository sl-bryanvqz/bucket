CREATE PROCEDURE [meta].[usp_get_tasks_due_for_execution]
    @stage VARCHAR(50) = NULL
AS
BEGIN
    -- DECLARE @stage VARCHAR(50);
    -- SET @stage = 'extract';
    SET NOCOUNT ON;
    DECLARE @now_utc DATETIME2(3) = SYSUTCDATETIME();

    BEGIN TRY
        DECLARE @cfg_errors TABLE (
            task_id INT,
            error_code VARCHAR(100),
            error_message NVARCHAR(4000),
            scheduling_settings NVARCHAR(MAX)
        );

        INSERT INTO @cfg_errors(task_id, error_code, error_message, scheduling_settings)
        EXEC meta.usp_validate_task_scheduling_settings @stage=@stage, @persist=1, @validator_version='v1';

        WITH valid_tasks AS (
            SELECT t.*
            FROM meta.task t
            LEFT JOIN @cfg_errors e ON e.task_id = t.task_id
            WHERE e.task_id IS NULL
              AND ISNULL(t.enabled,1) = 1
              AND (@stage IS NULL OR t.stage = @stage)
        ),
        last_success AS (
            SELECT  te.task_id, MAX(te.start_datetime) AS last_successful_run
            FROM    logging.task_executions te
            WHERE   te.status = 'Completed'
            GROUP BY te.task_id
        ),
        currently_running AS (
            SELECT DISTINCT te.task_id
            FROM   logging.task_executions te
            WHERE  te.status IS NULL OR te.status IN ('Running','In Progress')
        ),
        last_failed AS (
            SELECT  te.task_id, MAX(te.start_datetime) AS last_failed_run
            FROM    logging.task_executions te
            WHERE   te.status = 'Failed'
            GROUP BY te.task_id
        ),
        last_failed_details AS (
            SELECT  lf.task_id, lf.last_failed_run, te.num_retries AS last_failed_num_retries
            FROM    last_failed lf
            JOIN    logging.task_executions te
               ON   te.task_id = lf.task_id
              AND   te.start_datetime = lf.last_failed_run
              AND   te.status = 'Failed'
        ),
        tasks_enriched AS (
            SELECT
                NEWID() AS task_executions_id,
                NEWID() AS lineage_id,
                t.task_id,
                t.task_name,
                t.object_name,
                t.stage,
                t.source_connection_id,
                t.source_settings,
                t.target_settings,
                t.option_settings,
                t.template_settings,
                t.scheduling_settings,
                COALESCE(JSON_VALUE(t.scheduling_settings,'$.schedule_type'),'interval')                    AS schedule_type,
                JSON_QUERY(t.scheduling_settings,'$.scheduled_times')                                       AS scheduled_times_json,
                TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.frequency_minutes')            AS INT)         AS frequency_minutes,
                TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.skip_if_running')              AS BIT)         AS skip_if_running,
                TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.enable_retries')               AS BIT)         AS enable_retries,
                TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.max_retry_attempts')           AS INT)         AS max_retry_attempts,
                TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.day_of_month')                 AS INT)         AS day_of_month,
                TRY_CONVERT(time, JSON_VALUE(t.scheduling_settings,'$.time'))                               AS run_time,
                TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.month')                        AS INT)         AS month_num,
                TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.day')                          AS INT)         AS day_for_yearly,
                JSON_QUERY(t.scheduling_settings,'$.days_of_week')                                          AS days_of_week_json,
                ls.last_successful_run,
                DATEDIFF(MINUTE, ISNULL(ls.last_successful_run, '1900-01-01'), @now_utc)                    AS minutes_since_last_run,
                lfd.last_failed_run,
                ISNULL(lfd.last_failed_num_retries, 0)                                                      AS last_failed_num_retries,
                CASE WHEN cr.task_id IS NOT NULL THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END                AS is_currently_running,
                COALESCE(TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.frequency_minutes') AS INT), 1440)    AS eff_frequency_minutes,
                COALESCE(TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.skip_if_running')   AS BIT), 1)       AS eff_skip_if_running,
                COALESCE(TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.enable_retries')    AS BIT), 1)       AS eff_enable_retries,
                COALESCE(TRY_CAST(JSON_VALUE(t.scheduling_settings,'$.max_retry_attempts') AS INT), 0)      AS eff_max_retry_attempts
            FROM valid_tasks t
            LEFT JOIN last_success ls         ON ls.task_id = t.task_id
            LEFT JOIN last_failed_details lfd ON lfd.task_id = t.task_id
            LEFT JOIN currently_running cr    ON cr.task_id  = t.task_id
        ),
        tasks_due AS (
            SELECT
                te.*,
                CASE
                    WHEN te.eff_enable_retries = 1
                     AND te.last_failed_run IS NOT NULL
                     AND te.eff_max_retry_attempts > te.last_failed_num_retries
                     AND (te.is_currently_running = 0 OR te.eff_skip_if_running = 0)
                    THEN 1
                    WHEN te.schedule_type = 'interval'
                     AND te.minutes_since_last_run >= te.eff_frequency_minutes
                     AND (te.is_currently_running = 0 OR te.eff_skip_if_running = 0)
                    THEN 1
                    WHEN te.schedule_type = 'time_of_day'
                     AND te.scheduled_times_json IS NOT NULL
                     AND (te.is_currently_running = 0 OR te.eff_skip_if_running = 0)
                     AND EXISTS (
                            SELECT 1
                            FROM OPENJSON(te.scheduled_times_json) WITH (time_slot TIME '$') s
                            WHERE CAST(@now_utc AS TIME) >= s.time_slot
                              AND (
                                    te.last_successful_run IS NULL
                                 OR CAST(te.last_successful_run AS DATE) < CAST(@now_utc AS DATE)
                                 OR CAST(te.last_successful_run AS TIME) < s.time_slot
                                  )
                        )
                    THEN 1
                    WHEN te.schedule_type = 'weekly'
                     AND te.days_of_week_json IS NOT NULL
                     AND te.run_time IS NOT NULL
                     AND CAST(@now_utc AS TIME) >= te.run_time
                     AND (te.is_currently_running = 0 OR te.eff_skip_if_running = 0)
                     AND EXISTS (
                            SELECT 1
                            FROM OPENJSON(te.days_of_week_json) AS d
                            -- Convert SQL Server DATEPART(WEEKDAY) to ISO Mon=1..Sun=7
                            -- regardless of @@DATEFIRST setting
                            WHERE TRY_CAST(d.[value] AS INT)
                                  = (DATEPART(WEEKDAY, @now_utc) + @@DATEFIRST + 5) % 7 + 1
                        )
                     AND (
                            te.last_successful_run IS NULL
                         OR te.last_successful_run <
                            DATETIME2FROMPARTS(
                                DATEPART(YEAR,  @now_utc),
                                DATEPART(MONTH, @now_utc),
                                DATEPART(DAY,   @now_utc),
                                DATEPART(HOUR,   te.run_time),
                                DATEPART(MINUTE, te.run_time),
                                0, 0, 7
                            )
                        )
                    THEN 1
                    WHEN te.schedule_type = 'monthly'
                     AND te.day_of_month BETWEEN 1 AND 31
                     AND te.run_time IS NOT NULL
                     AND DAY(@now_utc) = te.day_of_month
                     AND CAST(@now_utc AS TIME) >= te.run_time
                     AND (te.is_currently_running = 0 OR te.eff_skip_if_running = 0)
                     AND (
                            te.last_successful_run IS NULL
                         OR te.last_successful_run <
                            DATETIME2FROMPARTS(
                                DATEPART(YEAR, @now_utc),
                                DATEPART(MONTH, @now_utc),
                                te.day_of_month,
                                DATEPART(HOUR, te.run_time),
                                DATEPART(MINUTE, te.run_time),
                                0, 0, 7
                            )
                        )
                    THEN 1
                    WHEN te.schedule_type = 'yearly'
                     AND te.month_num BETWEEN 1 AND 12
                     AND te.day_for_yearly BETWEEN 1 AND 31
                     AND te.run_time IS NOT NULL
                     AND DATEPART(MONTH, @now_utc) = te.month_num
                     AND DATEPART(DAY,   @now_utc) = te.day_for_yearly
                     AND CAST(@now_utc AS TIME) >= te.run_time
                     AND (te.is_currently_running = 0 OR te.eff_skip_if_running = 0)
                     AND (
                            te.last_successful_run IS NULL
                         OR te.last_successful_run <
                            DATETIME2FROMPARTS(
                                DATEPART(YEAR, @now_utc),
                                te.month_num,
                                te.day_for_yearly,
                                DATEPART(HOUR, te.run_time),
                                DATEPART(MINUTE, te.run_time),
                                0, 0, 7
                            )
                        )
                    THEN 1

                    ELSE 0
                END AS is_due
            FROM tasks_enriched te
        )
        SELECT
            T.task_executions_id,
            T.lineage_id,
            T.task_id,
            T.task_name,
            T.object_name,
            T.stage,
            T.source_connection_id,
            C.uses_fabric_connection AS source_uses_fabric_connection,
            C.connection_settings AS source_connection_settings,
            T.source_settings,
            T.target_settings,
            T.option_settings,
            T.template_settings,
            T.scheduling_settings,
            T.last_successful_run,
            T.last_failed_run,
            T.last_failed_num_retries,
            T.is_currently_running,
            T.schedule_type,
            T.eff_frequency_minutes,
            T.eff_skip_if_running,
            T.eff_enable_retries,
            T.eff_max_retry_attempts
        FROM tasks_due as T
        INNER JOIN meta.connections as C
            ON T.source_connection_id = C.connection_id
        WHERE is_due = 1;
        IF EXISTS (SELECT 1 FROM @cfg_errors)
        BEGIN
            INSERT INTO logging.task_executions
                ( task_executions_id
                , lineage_id
                , run_id
                , parent_run_id
                , task_id
                , task_name
                , stage
                , status
                , num_retries
                , start_date
                , start_datetime
                , end_datetime
                , template_name
                , workspace_id
                , log_data
                , monitoring_url
                , next_stage_status
                , object_name
                , error_details )
            SELECT  NEWID()                                   AS task_executions_id
                , NEWID()                                   AS lineage_id
                , NEWID()                                   AS run_id
                , NULL                                      AS parent_run_id
                , e.task_id
                , t.task_name
                , t.stage
                , 'ConfigError'                             AS status
                , 0                                         AS num_retries
                , CAST(@now_utc AS DATE)                    AS start_date
                , @now_utc                                  AS start_datetime
                , NULL                                      AS end_datetime
                , NULL                                      AS template_name
                , NULL                                      AS workspace_id
                , NULL                                      AS log_data
                , NULL                                      AS monitoring_url
                , NULL                                      AS next_stage_status
                , t.object_name
                , CONCAT(e.error_code, ': ', e.error_message) AS error_details
            FROM @cfg_errors e
            LEFT JOIN meta.task t
            ON t.task_id = e.task_id;
        END
    END TRY
    BEGIN CATCH
        INSERT INTO logging.task_config_errors(task_id, error_code, error_message, scheduling_settings_snapshot, validator_version)
        SELECT 0 AS task_id,
               'SCHEDULER_EXCEPTION',
               CONCAT('Proc failed: ', ERROR_MESSAGE()),
               NULL,
               'v1';
        -- Swallow the exception so the calling pipeline doesn't hard-fail on config mistakes
        -- but still returns an empty result set.
    END CATCH
END

GO

