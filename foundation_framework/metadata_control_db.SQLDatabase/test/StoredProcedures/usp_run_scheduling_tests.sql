CREATE PROCEDURE [test].[usp_run_scheduling_tests]
AS
BEGIN
    /*
    Runs a test suite for: meta.usp_validate_task_scheduling_settings

    What it does:
    - Inserts one synthetic task row per test into meta.task
    - Executes the validator with @persist = 0
    - Captures the returned result set
    - Evaluates PASS/FAIL based on expected error codes
    - Rolls back each test so no test data remains

    Assumptions:
    - meta.usp_validate_task_scheduling_settings returns:
        task_id, error_code, error_message, scheduling_settings
    - meta.task contains at least:
        task_id (identity or auto-generated),
        task_name,
        stage,
        enabled,
        scheduling_settings
    - Stages 'Load to bronze' and 'Load to silver' are excluded by design
    - Tests use stage = 'extract'
    */

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID('tempdb..#Tests') IS NOT NULL DROP TABLE #Tests;
    CREATE TABLE #Tests
    (
        test_id              INT IDENTITY(1,1) PRIMARY KEY,
        test_name            NVARCHAR(200) NOT NULL,
        stage_filter         VARCHAR(50) NULL,
        task_stage           VARCHAR(50) NOT NULL,
        enabled              BIT NOT NULL DEFAULT (1),
        scheduling_settings  NVARCHAR(MAX) NOT NULL,
        expect_error         BIT NOT NULL,
        expected_error_code  NVARCHAR(100) NULL,
        notes                NVARCHAR(400) NULL
    );

    -------------------------------------------------------------------------
    -- Happy path tests
    -------------------------------------------------------------------------
    INSERT INTO #Tests
    (
        test_name, stage_filter, task_stage, enabled, scheduling_settings,
        expect_error, expected_error_code, notes
    )
    VALUES
    (
        N'T1 Valid interval schedule',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"interval","frequency_minutes":15}',
        0,
        NULL,
        N'Valid interval schedule should return no validation errors.'
    ),
    (
        N'T2 Valid time_of_day schedule',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"time_of_day","scheduled_times":["09:00","17:30"]}',
        0,
        NULL,
        N'Valid time_of_day schedule should return no validation errors.'
    ),
    (
        N'T3 Valid weekly schedule',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"weekly","days_of_week":[1,3,5],"time":"10:00"}',
        0,
        NULL,
        N'Valid weekly schedule should return no validation errors.'
    ),
    (
        N'T4 Valid monthly schedule',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"monthly","day_of_month":15,"time":"08:00"}',
        0,
        NULL,
        N'Valid monthly schedule should return no validation errors.'
    ),
    (
        N'T5 Valid yearly schedule',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"yearly","month":12,"day":25,"time":"06:30"}',
        0,
        NULL,
        N'Valid yearly schedule should return no validation errors.'
    );

    -------------------------------------------------------------------------
    -- Negative tests
    -------------------------------------------------------------------------
    INSERT INTO #Tests
    (
        test_name, stage_filter, task_stage, enabled, scheduling_settings,
        expect_error, expected_error_code, notes
    )
    VALUES
    (
        N'T6 Invalid schedule_type',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"daily","time":"08:00"}',
        1,
        N'SCHEDULE_TYPE',
        N'Unsupported schedule_type should be rejected.'
    ),
    (
        N'T7 Interval missing frequency',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"interval"}',
        1,
        N'INTERVAL_MISSING_FREQUENCY',
        N'Interval requires positive frequency_minutes.'
    ),
    (
        N'T8 Interval non-positive frequency',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"interval","frequency_minutes":0}',
        1,
        N'INTERVAL_MISSING_FREQUENCY',
        N'frequency_minutes <= 0 should be rejected.'
    ),
    (
        N'T9 time_of_day missing scheduled_times',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"time_of_day"}',
        1,
        N'TIME_OF_DAY_EMPTY',
        N'time_of_day requires a non-empty scheduled_times array.'
    ),
    (
        N'T10 time_of_day invalid time',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"time_of_day","scheduled_times":["25:00"]}',
        1,
        N'TIME_OF_DAY_TIME_INVALID',
        N'Invalid time inside scheduled_times should be rejected.'
    ),
    (
        N'T11 weekly missing days_of_week',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"weekly","time":"10:00"}',
        1,
        N'WEEKLY_DAYS_OF_WEEK_EMPTY',
        N'Weekly requires days_of_week.'
    ),
    (
        N'T12 weekly invalid day_of_week',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"weekly","days_of_week":[0,8],"time":"10:00"}',
        1,
        N'WEEKLY_DAY_OF_WEEK_INVALID',
        N'Weekly allows only 1..7.'
    ),
    (
        N'T13 weekly invalid time',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"weekly","days_of_week":[1,2],"time":"99:99"}',
        1,
        N'WEEKLY_TIME_INVALID',
        N'Weekly requires valid HH:mm time.'
    ),
    (
        N'T14 monthly invalid day_of_month',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"monthly","day_of_month":32,"time":"08:00"}',
        1,
        N'MONTHLY_DAY_OF_MONTH_INVALID',
        N'Monthly day_of_month must be 1..31.'
    ),
    (
        N'T15 monthly invalid time',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"monthly","day_of_month":15,"time":"24:01"}',
        1,
        N'MONTHLY_TIME_INVALID',
        N'Monthly requires valid HH:mm time.'
    ),
    (
        N'T16 yearly invalid month',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"yearly","month":13,"day":10,"time":"06:30"}',
        1,
        N'YEARLY_MONTH_INVALID',
        N'Yearly month must be 1..12.'
    ),
    (
        N'T17 yearly invalid day',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"yearly","month":12,"day":0,"time":"06:30"}',
        1,
        N'YEARLY_DAY_INVALID',
        N'Yearly day must be 1..31.'
    ),
    (
        N'T18 yearly invalid time',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"yearly","month":12,"day":25,"time":"aa:bb"}',
        1,
        N'YEARLY_TIME_INVALID',
        N'Yearly requires valid HH:mm time.'
    ),
    (
        N'T19 yearly invalid date',
        NULL,
        'extract',
        1,
        N'{"schedule_type":"yearly","month":2,"day":30,"time":"06:30"}',
        1,
        N'YEARLY_INVALID_DATE',
        N'Invalid calendar date should be rejected.'
    );

    -------------------------------------------------------------------------
    -- Scope / exclusion behavior tests
    -------------------------------------------------------------------------
    INSERT INTO #Tests
    (
        test_name, stage_filter, task_stage, enabled, scheduling_settings,
        expect_error, expected_error_code, notes
    )
    VALUES
    (
        N'T20 Disabled task is ignored',
        NULL,
        'extract',
        0,
        N'{"schedule_type":"daily"}',
        0,
        NULL,
        N'Disabled tasks are filtered out and should produce no errors.'
    ),
    (
        N'T21 Excluded stage Load to bronze is ignored',
        NULL,
        'Load to bronze',
        1,
        N'{"schedule_type":"daily"}',
        0,
        NULL,
        N'Excluded stage should not be validated.'
    ),
    (
        N'T22 Stage filter excludes non-matching task',
        'publish',
        'extract',
        1,
        N'{"schedule_type":"daily"}',
        0,
        NULL,
        N'If @stage does not match task stage, task should not be validated.'
    ),
    (
        N'T23 Stage filter includes matching task',
        'extract',
        'extract',
        1,
        N'{"schedule_type":"daily"}',
        1,
        N'SCHEDULE_TYPE',
        N'If @stage matches task stage, invalid schedule should be returned.'
    );

    IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results;
    CREATE TABLE #Results
    (
        test_id               INT NOT NULL,
        test_name             NVARCHAR(200) NOT NULL,
        outcome               VARCHAR(10) NOT NULL,
        actual_error_code     NVARCHAR(100) NULL,
        actual_error_message  NVARCHAR(4000) NULL,
        expected_error_code   NVARCHAR(100) NULL,
        error_message         NVARCHAR(MAX) NULL,
        notes                 NVARCHAR(400) NULL
    );

    DECLARE
        @test_id             INT,
        @test_name           NVARCHAR(200),
        @stage_filter        VARCHAR(50),
        @task_stage          VARCHAR(50),
        @enabled             BIT,
        @scheduling_settings NVARCHAR(MAX),
        @expect_error        BIT,
        @expected_error_code NVARCHAR(100),
        @notes               NVARCHAR(400),
        @actual_error_code   NVARCHAR(100),
        @actual_error_msg    NVARCHAR(4000),
        @outcome             VARCHAR(10),
        @err                 NVARCHAR(MAX),
        @task_id             INT;

    DECLARE test_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            test_id,
            test_name,
            stage_filter,
            task_stage,
            enabled,
            scheduling_settings,
            expect_error,
            expected_error_code,
            notes
        FROM #Tests
        ORDER BY test_id;

    OPEN test_cur;
    FETCH NEXT FROM test_cur INTO
        @test_id, @test_name, @stage_filter, @task_stage, @enabled,
        @scheduling_settings, @expect_error, @expected_error_code, @notes;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @actual_error_code = NULL;
        SET @actual_error_msg  = NULL;
        SET @outcome           = 'FAIL';
        SET @err               = NULL;
        SET @task_id           = NULL;

        BEGIN TRY
            BEGIN TRANSACTION;

            /*
              IMPORTANT:
              Adjust this insert if meta.task has additional required columns.
            */
            INSERT INTO meta.task
            (
                task_name,
                stage,
                enabled,
                scheduling_settings
            )
            VALUES
            (
                CONCAT('UT_validate_sched_', @test_id),
                @task_stage,
                @enabled,
                @scheduling_settings
            );

            SET @task_id = SCOPE_IDENTITY();

            DECLARE @ProcOut TABLE
            (
                task_id              INT,
                error_code           NVARCHAR(100),
                error_message        NVARCHAR(4000),
                scheduling_settings  NVARCHAR(MAX)
            );

            INSERT INTO @ProcOut (task_id, error_code, error_message, scheduling_settings)
            EXEC meta.usp_validate_task_scheduling_settings
                @stage = @stage_filter,
                @persist = 0,
                @validator_version = 'unit-test';

            SELECT TOP (1)
                @actual_error_code = p.error_code,
                @actual_error_msg  = p.error_message
            FROM @ProcOut p
            WHERE p.task_id = @task_id
            ORDER BY p.error_code;

            IF @expect_error = 1
            BEGIN
                IF @actual_error_code = @expected_error_code
                    SET @outcome = 'PASS';
                ELSE
                    SET @err = CONCAT(
                        'Expected error code ',
                        ISNULL(@expected_error_code, '<null>'),
                        ', but got ',
                        ISNULL(@actual_error_code, '<none>')
                    );
            END
            ELSE
            BEGIN
                IF @actual_error_code IS NULL
                    SET @outcome = 'PASS';
                ELSE
                    SET @err = CONCAT(
                        'Expected no validation error, but got ',
                        @actual_error_code
                    );
            END

            ROLLBACK TRANSACTION;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;

            SET @err = CONCAT('Error ', ERROR_NUMBER(), ': ', ERROR_MESSAGE());
            SET @outcome = 'FAIL';
        END CATCH;

        INSERT INTO #Results
        (
            test_id,
            test_name,
            outcome,
            actual_error_code,
            actual_error_message,
            expected_error_code,
            error_message,
            notes
        )
        VALUES
        (
            @test_id,
            @test_name,
            @outcome,
            @actual_error_code,
            @actual_error_msg,
            @expected_error_code,
            @err,
            @notes
        );

        FETCH NEXT FROM test_cur INTO
            @test_id, @test_name, @stage_filter, @task_stage, @enabled,
            @scheduling_settings, @expect_error, @expected_error_code, @notes;
    END

    CLOSE test_cur;
    DEALLOCATE test_cur;

    SELECT
        test_id,
        test_name,
        outcome,
        actual_error_code,
        actual_error_message,
        expected_error_code,
        error_message,
        notes
    FROM #Results
    ORDER BY test_id;

    SELECT
        SUM(CASE WHEN outcome = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN outcome = 'FAIL' THEN 1 ELSE 0 END) AS failed,
        COUNT(*) AS total
    FROM #Results;
END;
GO