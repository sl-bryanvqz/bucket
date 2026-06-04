CREATE PROCEDURE [test].[usp_run_sql_query_parser_tests]
AS
BEGIN
    /*
    Runs a test suite for: meta.usp_sql_query_parser

    - Executes tests
    - Captures the stored procedure output (resolved_source_query)
    - Evaluates PASS/FAIL with simple assertions (LIKE / EQUAL / NOTLIKE)
    - Returns:
        1) Detailed results set
        2) Summary results set

    Assumptions:
    - meta.usp_sql_query_parser returns a single-row, single-column result set
      with column name: resolved_source_query
    - Missing-token behavior: unresolved tokens are ALLOWED ($missing remains).
      If you later change the parser proc to THROW on unresolved tokens, flip
      that test to expect_error = 1.
    */

    SET NOCOUNT ON;

    IF OBJECT_ID('tempdb..#Tests') IS NOT NULL DROP TABLE #Tests;
    CREATE TABLE #Tests
    (
        test_id      INT IDENTITY(1,1) PRIMARY KEY,
        test_name    NVARCHAR(200) NOT NULL,
        input_json   NVARCHAR(MAX) NOT NULL,
        expect_error BIT NOT NULL DEFAULT (0),
        assert_type  VARCHAR(20) NULL,         -- 'LIKE' | 'EQUAL' | 'NOTLIKE' | NULL (if expect_error=1)
        assert_value NVARCHAR(MAX) NULL,       -- pattern or exact expected string
        notes        NVARCHAR(400) NULL
    );

    -- Test 1 — Happy Path tokens updated.
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T1 Happy path TODAY_SAP1',
     N'{"source_query":"SELECT * FROM $main_table WHERE $varb1 >= ''$start_date''",
        "values":[{"main_table":"SAPCRM.CRMD_ISUEXTA4"},
                  {"varb1":"A4CONTSTART"},
                  {"start_date":"TODAY_SAP1()"}]}',
     0,
     'LIKE',
     N'%SELECT * FROM SAPCRM.CRMD_ISUEXTA4 WHERE A4CONTSTART >= ''%',
     N'Validates token replacement occurred (table/column).'
    );

    -- Test 2 - Updated TODAY_SAP1() with a value
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T2 TODAY_SAP1 expanded (no literal remains)',
     N'{"source_query":"SELECT * FROM $main_table WHERE $varb1 >= ''$start_date''",
        "values":[{"main_table":"SAPCRM.CRMD_ISUEXTA4"},
                  {"varb1":"A4CONTSTART"},
                  {"start_date":"TODAY_SAP1()"}]}',
     0,
     'NOTLIKE',
     N'%TODAY_SAP1()%',
     N'Validates keyword expansion occurred (TODAY_SAP1() should not remain in the resolved query).'
    );

    -- Test 3 — NOW() returns Mountain time (ISO with offset -06:00 or -07:00)
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T3 NOW() Mountain time ISO',
     N'{"source_query":"SELECT ''$now_val'' AS now_val",
        "values":[{"now_val":"NOW()"}]}',
     0,
     'LIKE',
     N'%SELECT ''____-__-__T__:__:__.___%'' AS now_val%',
     N'Validates NOW() expands to an ISO-like datetime string.'
    );

    -- Test 4 — NOW_SAP2() => yyyymmddHHMMSS (14 digits)
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T4 NOW_SAP2 14-digit timestamp',
     N'{"source_query":"SELECT ''$ts'' AS sap2_ts",
        "values":[{"ts":"NOW_SAP2()"}]}',
     0,
     'LIKE',
     N'SELECT ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' AS sap2_ts',
     N'Validates yyyymmddHHMMSS (14 digits)'
    );

    -- Test 5 — ${token} syntax support (if your proc supports it)
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T5 ${token} syntax',
     N'{"source_query":"SELECT * FROM ${main_table} WHERE ${varb1} >= ''${start_date}''",
        "values":[{"main_table":"SAPCRM.CRMD_ISUEXTA4"},
                  {"varb1":"A4CONTSTART"},
                  {"start_date":"TODAY_SAP1()"}]}',
     0,
     'LIKE',
     N'%SELECT * FROM SAPCRM.CRMD_ISUEXTA4 WHERE A4CONTSTART >= ''%',
     N'Validates ${token} replacement occurred (table/column).'
    );

    -- Test 6 — Overlapping tokens (id vs id2) / longest-first ordering
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T6 Overlapping tokens id/id2',
     N'{"source_query":"SELECT $id AS id_val, $id2 AS id2_val",
        "values":[{"id":"X"},{"id2":"Y"}]}',
     0,
     'EQUAL',
     N'SELECT X AS id_val, Y AS id2_val',
     N'Validates ordering avoids partial overlap issues'
    );

    -- Test 7 — Token appears multiple times (3 occurrences)
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T7 Token appears multiple times',
     N'{"source_query":"SELECT ''$x'' AS a, ''$x'' AS b, ''$x'' AS c",
        "values":[{"x":"TODAY_SAP1()"}]}',
     0,
     'LIKE',
     N'SELECT ''[0-3][0-9].[0-1][0-9].[1-2][0-9][0-9][0-9]'' AS a, ''[0-3][0-9].[0-1][0-9].[1-2][0-9][0-9][0-9]'' AS b, ''[0-3][0-9].[0-1][0-9].[1-2][0-9][0-9][0-9]'' AS c',
     N'Validates global replace of all occurrences'
    );

    -- Test 8 — Missing token mapping (unresolved allowed -> token remains)
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T8 Missing token mapping remains unresolved',
     N'{"source_query":"SELECT * FROM $main_table WHERE col = ''$missing''",
        "values":[{"main_table":"SAPCRM.CRMD_ISUEXTA4"}]}',
     0,
     'LIKE',
     N'%$missing%',
     N'Validates behavior when token not provided'
    );

    -- Test 9 — Empty string token value
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T9 Empty string token value',
     N'{"source_query":"SELECT ''$v'' AS val",
        "values":[{"v":""}]}',
     0,
     'EQUAL',
     N'SELECT '''' AS val',
     N'Validates empty string replacement'
    );

    -- Test 10 — Invalid JSON should throw
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T10 Invalid JSON throws',
     N'{"source_query":"SELECT 1", "values": [ {"a":"b"} ]',
     1,
     NULL,
     NULL,
     N'Validates JSON input validation'
    );

    -- Test 11 — Missing source_query should throw
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T11 Missing source_query throws',
     N'{"values":[{"a":"b"}]}',
     1,
     NULL,
     NULL,
     N'Validates required field enforcement'
    );

    -- Test 12 — Blank SQL query with no values
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T12 Blank SQL query with no values',
     N'{"source_query":"","values":[]}',
     1,
     'EQUAL',
     N'',
     N'Validates handling of blank source_query with empty values array'
    );

    -- Test 13 — SQL query without tokens and empty values array
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T13 SQL without tokens, empty values',
     N'{"source_query":"SELECT CustomerID, CustomerName FROM Customers WHERE Country = ''USA''","values":[]}',
     0,
     'EQUAL',
     N'SELECT CustomerID, CustomerName FROM Customers WHERE Country = ''USA''',
     N'Validates that SQL without tokens returns unchanged when no values provided'
    );

    -- Test 14 — WATERMARK token with missing watermark entry
    INSERT INTO logging.execution_watermark (task_id, source_table, incremental_column, watermark_value, ingestion_timestamp)
    VALUES (99999999, 'ORDERS', 'OrderDate', '2024-01-01T00:00:00.0000000Z', SYSUTCDATETIME());  -- dummy entry for other tests
    INSERT INTO #Tests (test_name, input_json, expect_error, assert_type, assert_value, notes)
    VALUES
    (
     N'T14 WATERMARK token with missing watermark entry',
     N'{"source_query":"SELECT * FROM Orders WHERE OrderDate > ''$wm''",
        "values":[{"wm":"WATERMARK(99999999_ORDERS)"}]}',
     0,
     'EQUAL',
     'SELECT * FROM Orders WHERE OrderDate > ''2024-01-01T00:00:00.0000000Z''',
     N'Validates error thrown for missing watermark entry'
    );

    IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results;
    CREATE TABLE #Results
    (
        test_id       INT NOT NULL,
        test_name     NVARCHAR(200) NOT NULL,
        outcome       VARCHAR(10) NOT NULL,     -- PASS/FAIL
        actual_output NVARCHAR(MAX) NULL,
        expected      NVARCHAR(MAX) NULL,
        error_message NVARCHAR(MAX) NULL,
        notes         NVARCHAR(400) NULL
    );

    DECLARE
        @test_id INT,
        @test_name NVARCHAR(200),
        @input_json NVARCHAR(MAX),
        @expect_error BIT,
        @assert_type VARCHAR(20),
        @assert_value NVARCHAR(MAX),
        @notes NVARCHAR(400),
        @actual NVARCHAR(MAX),
        @outcome VARCHAR(10),
        @err NVARCHAR(MAX);

    -- Must match parser proc output column name
    DECLARE @ProcOut TABLE (resolved_source_query NVARCHAR(MAX));

    DECLARE test_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT test_id, test_name, input_json, expect_error, assert_type, assert_value, notes
        FROM #Tests
        ORDER BY test_id;

    OPEN test_cur;
    FETCH NEXT FROM test_cur INTO @test_id, @test_name, @input_json, @expect_error, @assert_type, @assert_value, @notes;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @actual = NULL;
        SET @outcome = 'FAIL';
        SET @err = NULL;

        BEGIN TRY
            DELETE FROM @ProcOut;

            INSERT INTO @ProcOut (resolved_source_query)
            EXEC meta.usp_sql_query_parser @input_json, @task_id = 99999999;  -- dummy task_id for watermark checks

            SELECT TOP (1) @actual = resolved_source_query FROM @ProcOut;

            IF @expect_error = 1
            BEGIN
                SET @outcome = 'FAIL';
                SET @err = 'Expected error, but procedure succeeded.';
            END
            ELSE
            BEGIN
                IF @assert_type = 'EQUAL'
                BEGIN
                    IF LTRIM(RTRIM(ISNULL(@actual,N''))) = LTRIM(RTRIM(ISNULL(@assert_value,N'')))
                        SET @outcome = 'PASS';
                END
                ELSE IF @assert_type = 'LIKE'
                BEGIN
                    IF @actual LIKE @assert_value
                        SET @outcome = 'PASS';
                END
                ELSE IF @assert_type = 'NOTLIKE'
                BEGIN
                    IF @actual NOT LIKE @assert_value
                        SET @outcome = 'PASS';
                END
                ELSE
                BEGIN
                    SET @outcome = 'FAIL';
                    SET @err = 'No assert_type provided for non-error test.';
                END
            END
        END TRY
        BEGIN CATCH
            SET @err = CONCAT('Error ', ERROR_NUMBER(), ': ', ERROR_MESSAGE());

            IF @expect_error = 1
                SET @outcome = 'PASS';
            ELSE
                SET @outcome = 'FAIL';
        END CATCH;

        INSERT INTO #Results (test_id, test_name, outcome, actual_output, expected, error_message, notes)
        VALUES (@test_id, @test_name, @outcome, @actual, @assert_value, @err, @notes);

        FETCH NEXT FROM test_cur INTO @test_id, @test_name, @input_json, @expect_error, @assert_type, @assert_value, @notes;
    END

    CLOSE test_cur;
    DEALLOCATE test_cur;

    SELECT
        r.test_id,
        r.test_name,
        r.outcome,
        r.actual_output,
        r.expected,
        r.error_message,
        r.notes,
        t.input_json
    FROM #Results r
    INNER JOIN #Tests t ON r.test_id = t.test_id
    ORDER BY r.test_id;

    SELECT
        SUM(CASE WHEN outcome = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN outcome = 'FAIL' THEN 1 ELSE 0 END) AS failed,
        COUNT(*) AS total
    FROM #Results;

    DELETE FROM logging.execution_watermark WHERE task_id = 99999999 AND source_table = 'ORDERS';

END;

GO

