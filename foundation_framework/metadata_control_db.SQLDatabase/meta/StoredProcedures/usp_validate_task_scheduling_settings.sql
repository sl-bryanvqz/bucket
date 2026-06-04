CREATE PROCEDURE [meta].[usp_validate_task_scheduling_settings]
  @stage                VARCHAR(50) NULL    ,
  @persist              BIT         = 1     ,
  @validator_version    VARCHAR(20) = 'v1'
AS
  -- DECLARE @stage VARCHAR(50);
  -- DECLARE @persist BIT;
  -- DECLARE @validator_version VARCHAR(20);
  -- SET @persist = 1;
  -- SET @validator_version = 'v1';
  -- SET @stage = 'extract';
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('tempdb..#problems') IS NOT NULL DROP TABLE #problems;
    CREATE TABLE #problems
    (
        task_id               INT         NOT NULL,
        error_code            NVARCHAR(100) NOT NULL,
        error_message         NVARCHAR(4000) NOT NULL,
        scheduling_settings   NVARCHAR(MAX)  NULL
    );

    ;WITH t AS (
        SELECT
            tk.task_id,
            tk.task_name,
            tk.stage,
            tk.scheduling_settings,
            COALESCE(JSON_VALUE(tk.scheduling_settings, '$.schedule_type'), 'interval') AS schedule_type,
            TRY_CAST(JSON_VALUE(tk.scheduling_settings, '$.frequency_minutes') AS INT)  AS frequency_minutes,
            JSON_QUERY(tk.scheduling_settings, '$.scheduled_times')                     AS scheduled_times_json,
            TRY_CAST(JSON_VALUE(tk.scheduling_settings, '$.day_of_month') AS INT)       AS day_of_month,
            JSON_VALUE(tk.scheduling_settings, '$.time')                                AS run_time_text,
            TRY_CAST(JSON_VALUE(tk.scheduling_settings, '$.month') AS INT)              AS month_num,
            TRY_CAST(JSON_VALUE(tk.scheduling_settings, '$.day')   AS INT)              AS day_for_yearly,
            JSON_QUERY(tk.scheduling_settings, '$.days_of_week')                        AS days_of_week_json
        FROM meta.task tk
        WHERE ISNULL(tk.enabled,1) = 1
          AND (@stage IS NULL OR tk.stage = @stage)
          And tk.stage not in ('Load to bronze','Load to silver')
    ),
    times AS (
        SELECT
            t.task_id,
            j.[value] AS time_text
        FROM t
        CROSS APPLY OPENJSON(
            CASE WHEN t.schedule_type = 'time_of_day' THEN t.scheduled_times_json ELSE N'[]' END
        ) AS j
    ),
    weekly_days AS (
        SELECT
            t.task_id,
            TRY_CAST(j.[value] AS INT) AS day_of_week
        FROM t
        CROSS APPLY OPENJSON(
            CASE WHEN t.schedule_type = 'weekly' THEN t.days_of_week_json ELSE N'[]' END
        ) AS j
    ),
    invalid AS (
        SELECT
            t.task_id,
            CAST('SCHEDULE_TYPE' AS NVARCHAR(100)) AS error_code,
            CONCAT('Invalid schedule_type: ', ISNULL(t.schedule_type,'<null>')) AS error_message
        FROM t
        WHERE t.schedule_type NOT IN ('interval','time_of_day','weekly','monthly','yearly')

        UNION ALL

        SELECT
            t.task_id,
            'INTERVAL_MISSING_FREQUENCY',
            'frequency_minutes must be a positive integer for interval schedules'
        FROM t
        WHERE t.schedule_type = 'interval'
          AND (t.frequency_minutes IS NULL OR t.frequency_minutes <= 0)

        UNION ALL

        SELECT
            t.task_id,
            'TIME_OF_DAY_EMPTY',
            'scheduled_times must be a non-empty array of HH:mm strings'
        FROM t
        WHERE t.schedule_type = 'time_of_day'
          AND (t.scheduled_times_json IS NULL OR t.scheduled_times_json = N'[]')

        UNION ALL
        SELECT
            t.task_id,
            'TIME_OF_DAY_TIME_INVALID',
            CONCAT('Invalid time in scheduled_times: "', COALESCE(times.time_text,'<null>'), '" (expected HH:mm)')
        FROM t
        INNER JOIN times ON times.task_id = t.task_id
        WHERE t.schedule_type = 'time_of_day'
          AND TRY_CONVERT(time, times.time_text) IS NULL

        UNION ALL

        SELECT
            t.task_id,
            'WEEKLY_DAYS_OF_WEEK_EMPTY',
            'days_of_week must be a non-empty array of integers 1-7 for weekly schedules'
        FROM t
        WHERE t.schedule_type = 'weekly'
          AND (t.days_of_week_json IS NULL OR t.days_of_week_json = N'[]')

        UNION ALL

        SELECT DISTINCT
            t.task_id,
            'WEEKLY_DAY_OF_WEEK_INVALID',
            CONCAT(
                'Invalid day in days_of_week: "',
                COALESCE(CAST(wd.day_of_week AS VARCHAR(10)), '<null>'),
                '" (expected integer 1-7, where 1=Monday and 7=Sunday)'
            )
        FROM t
        INNER JOIN weekly_days wd
            ON wd.task_id = t.task_id
        WHERE t.schedule_type = 'weekly'
          AND (wd.day_of_week IS NULL OR wd.day_of_week < 1 OR wd.day_of_week > 7)

        UNION ALL
        SELECT
                t.task_id,
                'WEEKLY_TIME_INVALID',
                'time must be provided as HH:mm for weekly schedules'
        FROM t
        WHERE t.schedule_type = 'weekly'
            AND (t.run_time_text IS NULL OR TRY_CONVERT(time, t.run_time_text) IS NULL)

        UNION ALL

        SELECT
            t.task_id,
            'MONTHLY_DAY_OF_MONTH_INVALID',
            'day_of_month must be between 1 and 31 for monthly schedules'
        FROM t
        WHERE t.schedule_type = 'monthly'
          AND (t.day_of_month IS NULL OR t.day_of_month < 1 OR t.day_of_month > 31)

        UNION ALL
        SELECT
            t.task_id,
            'MONTHLY_TIME_INVALID',
            'time must be provided as HH:mm for monthly schedules'
        FROM t
        WHERE t.schedule_type = 'monthly'
          AND (t.run_time_text IS NULL OR TRY_CONVERT(time, t.run_time_text) IS NULL)

        UNION ALL

        SELECT
            t.task_id,
            'YEARLY_MONTH_INVALID',
            'month must be between 1 and 12 for yearly schedules'
        FROM t
        WHERE t.schedule_type = 'yearly'
          AND (t.month_num IS NULL OR t.month_num < 1 OR t.month_num > 12)

        UNION ALL
        SELECT
            t.task_id,
            'YEARLY_DAY_INVALID',
            'day must be between 1 and 31 for yearly schedules'
        FROM t
        WHERE t.schedule_type = 'yearly'
          AND (t.day_for_yearly IS NULL OR t.day_for_yearly < 1 OR t.day_for_yearly > 31)

        UNION ALL
        SELECT
            t.task_id,
            'YEARLY_TIME_INVALID',
            'time must be provided as HH:mm for yearly schedules'
        FROM t
        WHERE t.schedule_type = 'yearly'
          AND (t.run_time_text IS NULL OR TRY_CONVERT(time, t.run_time_text) IS NULL)

        UNION ALL
        /* Yearly month/day combo must be a real calendar date.
           Use leap year 2000 so Feb 29 is considered valid (executes only in leap years). */
        SELECT
            t.task_id,
            'YEARLY_INVALID_DATE',
            CONCAT(
                'Invalid month/day for yearly schedule: month=',
                ISNULL(CAST(t.month_num AS varchar(2)),'<null>'),
                ', day=',
                ISNULL(CAST(t.day_for_yearly AS varchar(2)),'<null>')
            )
        FROM t
        WHERE t.schedule_type = 'yearly'
          AND (
                                t.month_num BETWEEN 1 AND 12
                                AND t.day_for_yearly BETWEEN 1 AND 31
                                AND
                TRY_CONVERT(
                    date,
                    CONCAT(
                        '2000-',
                        RIGHT('0' + CAST(t.month_num AS varchar(2)), 2), '-',
                        RIGHT('0' + CAST(t.day_for_yearly AS varchar(2)), 2)
                    )
                ) IS NULL
              )
    )
    INSERT INTO #problems (task_id, error_code, error_message, scheduling_settings)
    SELECT i.task_id, i.error_code, i.error_message, t.scheduling_settings
    FROM invalid i
    JOIN t ON t.task_id = i.task_id;

    IF @persist = 1
    BEGIN
        INSERT INTO logging.task_config_errors
        (
            task_id,
            error_code,
            error_message,
            scheduling_settings_snapshot,
            validator_version,
            detected_at
        )
        SELECT
            p.task_id,
            p.error_code,
            p.error_message,
            p.scheduling_settings,
            @validator_version,
            SYSUTCDATETIME()
        FROM #problems p;
    END

    SELECT * FROM #problems;
END

GO

