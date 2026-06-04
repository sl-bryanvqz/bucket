CREATE PROCEDURE [meta].[usp_sql_query_parser]
  @source_settings NVARCHAR(MAX),
  @task_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @task_id IS NULL
    BEGIN
    ; THROW 50002, '@task_id is required.', 1;
    END;

    IF ISJSON(@source_settings) <> 1
    BEGIN
        DECLARE @msg NVARCHAR(2048);
        DECLARE @payload NVARCHAR(1800) =
            CASE
                WHEN @source_settings IS NULL THEN N'<NULL>'
                WHEN LEN(@source_settings) > 1800 THEN LEFT(@source_settings, 1800) + N'...<truncated>'
                ELSE @source_settings
            END;
        SET @msg = N'@source_settings must be valid JSON. Payload=' + @payload;
        THROW 50000, @msg, 1;
    END;

    DECLARE @query NVARCHAR(MAX) = JSON_VALUE(@source_settings, '$.source_query');
    DECLARE @ISERR INT = 0;

    IF @query IS NULL OR LTRIM(RTRIM(@query)) = ''
    BEGIN
    ;  THROW 50001, 'JSON property $.source_query is required.', 1;
    END;

    DECLARE @Tokens TABLE
    (
        TokenName  NVARCHAR(200) NOT NULL,
        TokenValue NVARCHAR(MAX) NULL
    );

    /*
    Allows:
    "values": [ {"main_table":"SAPCRM.CRMD_ISUEXTA4"}, {"start_date":"2026-01-01"} ]
    "values": { "start_date" : "1900-01-01","end_date":"2000-12-31" }
    */
    DECLARE @valuesJson NVARCHAR(MAX) = JSON_QUERY(@source_settings, '$.values');

    IF @valuesJson IS NOT NULL
    BEGIN
        IF LEFT(LTRIM(@valuesJson), 1) = '['
        BEGIN
            -- Handle array
            INSERT INTO @Tokens (TokenName, TokenValue)
            SELECT kv.[key] AS TokenName,
                    CAST(kv.[value] AS NVARCHAR(MAX)) AS TokenValue
            FROM OPENJSON(@valuesJson) AS v
            CROSS APPLY OPENJSON(v.[value]) AS kv;
        END
        ELSE IF LEFT(LTRIM(@valuesJson), 1) = '{'
        BEGIN
            -- Handle list
            INSERT INTO @Tokens (TokenName, TokenValue)
            SELECT [key],
                    CAST([value] AS NVARCHAR(MAX))
            FROM OPENJSON(@valuesJson);
        END
        ELSE
        BEGIN
            ; THROW 50003, 'JSON property $.values must be a list (object) or an array.', 1;
        END
    END;

    DECLARE @nowUtc DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @nowMtn DATETIMEOFFSET(3) = (@nowUtc AT TIME ZONE 'UTC') AT TIME ZONE 'Mountain Standard Time';

    ;WITH TokenNorm AS
    (
        SELECT  t.TokenName,
                t.TokenValue,
                NormValue = UPPER(LTRIM(RTRIM(t.TokenValue)))
            FROM  @Tokens t
            WHERE  t.TokenValue IS NOT NULL
    )
    UPDATE  t
        SET  t.TokenValue =
        CASE  -- Mountain time (DST-aware). Prefer ISO 8601 with offset.
            WHEN UPPER(LTRIM(RTRIM(t.TokenValue))) = 'NOW()'
                THEN CONVERT(NVARCHAR(33), @nowMtn, 127)
            -- SAP1: dd.mm.yyyy
            WHEN UPPER(LTRIM(RTRIM(t.TokenValue))) = 'TODAY_SAP1()'
                THEN CONVERT(NVARCHAR(10), CONVERT(DATE, @nowMtn), 104)
            WHEN UPPER(LTRIM(RTRIM(t.TokenValue))) = 'YESTERDAY_SAP1()'
                THEN CONVERT(NVARCHAR(10), DATEADD(DAY, -1, CONVERT(DATE, @nowMtn)), 104)
            WHEN UPPER(LTRIM(RTRIM(t.TokenValue))) = 'TOMORROW_SAP1()'
                THEN CONVERT(NVARCHAR(10), DATEADD(DAY,  1, CONVERT(DATE, @nowMtn)), 104)
            WHEN UPPER(LTRIM(RTRIM(t.TokenValue))) = 'NOW_SAP1()'
                THEN CONVERT(NVARCHAR(10), CONVERT(DATE, @nowMtn), 104)
            -- SAP2: yyyymmddHHMMSS
            WHEN UPPER(LTRIM(RTRIM(t.TokenValue))) = 'NOW_SAP2()'
                THEN CONVERT(NVARCHAR(8), CAST(@nowMtn AS DATETIME2(0)), 112)
                    + REPLACE(CONVERT(NVARCHAR(8), CAST(@nowMtn AS TIME(0)), 108), ':', '')
            WHEN UPPER(LTRIM(RTRIM(t.TokenValue))) = 'TODAY_SAP2()'
                THEN CONVERT(NVARCHAR(8), CONVERT(DATE, @nowMtn), 112)
            WHEN UPPER(LTRIM(RTRIM(t.TokenValue))) = 'YESTERDAY_SAP2()'
                THEN CONVERT(NVARCHAR(8), DATEADD(DAY, -1, CONVERT(DATE, @nowMtn)), 112)
            WHEN UPPER(LTRIM(RTRIM(t.TokenValue))) = 'TOMORROW_SAP2()'
                THEN CONVERT(NVARCHAR(8), DATEADD(DAY,  1, CONVERT(DATE, @nowMtn)), 112)
            -- Watermark lookup: WATERMARK(<TASKID>_<TABLENAME>)
            WHEN tn.NormValue LIKE 'WATERMARK(%' AND RIGHT(tn.NormValue, 1) = ')'
                THEN wm.watermark_value
            ELSE t.TokenValue
        END
        FROM  @Tokens t
        JOIN  TokenNorm tn
        ON  tn.TokenName = t.TokenName
        OUTER  APPLY (    -- Parse inside parentheses <TASKID>_<TABLENAME>
                SELECT  inside = CASE
                    WHEN LEN(t.TokenValue) > LEN('WATERMARK()')
                    THEN SUBSTRING(t.TokenValue, LEN('WATERMARK(') + 1, LEN(t.TokenValue) - LEN('WATERMARK(') - 1)
                    ELSE NULL
                END
            ) p
        OUTER  APPLY (    -- Split into task and table parts
                SELECT  us_pos      = NULLIF(CHARINDEX('_', p.inside), 0),
                        token_task  = TRY_CONVERT(BIGINT, CASE WHEN CHARINDEX('_', p.inside) > 0 THEN LEFT(p.inside, CHARINDEX('_', p.inside) - 1) END),
                        token_table = CASE WHEN CHARINDEX('_', p.inside) > 0 THEN SUBSTRING(p.inside, CHARINDEX('_', p.inside) + 1, LEN(p.inside)) END
            ) x
        OUTER  APPLY (     -- Look up the latest watermark (only when parse is valid)
                SELECT  TOP (1)
                        watermark_value       = CAST(w.watermark_value AS NVARCHAR(MAX)),
                        ingestion_timestamp   = w.ingestion_timestamp
                    FROM  logging.execution_watermark w
                    WHERE  w.task_id = @task_id
                    AND  w.source_table = x.token_table
                    ORDER  BY  w.ingestion_timestamp DESC
            ) wm
        WHERE  t.TokenValue IS NOT NULL;

    -- Token content validation blank / malformed WATERMARK()
    IF EXISTS   (
        SELECT  1
            FROM  @Tokens t
            CROSS  APPLY   (
            SELECT  NormValue = UPPER(LTRIM(RTRIM(t.TokenValue)))) n
                CROSS  APPLY   (
                SELECT  inside = CASE
                    WHEN n.NormValue LIKE 'WATERMARK(%' AND RIGHT(n.NormValue,1)=')' THEN
                        SUBSTRING(t.TokenValue, LEN('WATERMARK(') + 1, LEN(t.TokenValue) - LEN('WATERMARK(') - 1)
                END
            ) p
            WHERE  n.NormValue LIKE 'WATERMARK(%'
            AND  RIGHT(n.NormValue,1)=')'
            AND  (
                        p.inside IS NULL OR LTRIM(RTRIM(p.inside)) = N''       -- blank WATERMARK()
                    OR  CHARINDEX('_', p.inside) = 0                           -- missing underscore
            )
    )
    BEGIN
        ; THROW 50010, 'Invalid WATERMARK() token. Expected WATERMARK(<TASKID>_<TABLENAME>) and content must not be blank.', 1;
    END
    ;
    -- Token content validation missing watermark entry
    IF EXISTS
    (
        SELECT  1
            FROM  @Tokens t
            CROSS  APPLY   (
            SELECT  NormValue = UPPER(LTRIM(RTRIM(t.TokenValue)))) n
                CROSS  APPLY   (
                SELECT inside = CASE
                    WHEN  n.NormValue LIKE 'WATERMARK(%' AND RIGHT(n.NormValue,1)=')' AND LEN(t.TokenValue) > LEN('WATERMARK()') THEN
                    SUBSTRING(t.TokenValue, LEN('WATERMARK(') + 1, LEN(t.TokenValue) - LEN('WATERMARK(') - 1)
                END
            ) p
            CROSS   APPLY   (
                SELECT  token_table =   CASE
                    WHEN  CHARINDEX('_', p.inside) > 0 THEN
                    SUBSTRING(p.inside, CHARINDEX('_', p.inside) + 1, LEN(p.inside))
                END
            ) x
            WHERE n.NormValue LIKE 'WATERMARK(%'
            AND RIGHT(n.NormValue,1)=')'
            AND x.token_table IS NOT NULL
            AND NOT EXISTS  (
                SELECT  1
                    FROM  logging.execution_watermark w
                    WHERE  w.task_id = @task_id
                    AND  w.source_table = x.token_table
            )
    )
    BEGIN
        DECLARE @missing NVARCHAR(400);

        SELECT TOP (1) @missing =
            CASE
                WHEN LEN(t.TokenValue) > LEN('WATERMARK()')
                THEN SUBSTRING(t.TokenValue, LEN('WATERMARK(') + 1, LEN(t.TokenValue) - LEN('WATERMARK(') - 1)
                ELSE t.TokenValue
            END
        FROM @Tokens t
        CROSS APPLY (SELECT NormValue = UPPER(LTRIM(RTRIM(t.TokenValue)))) n
        WHERE n.NormValue LIKE 'WATERMARK(%' AND RIGHT(n.NormValue,1)=')';

        DECLARE @error_msg NVARCHAR(2048) = 'No watermark found for task_id=' +
            CAST(@task_id AS NVARCHAR(10)) +
            ' and token=' + @missing + '.';
        THROW 50011, @error_msg, 1;
    END;


    -- Here we're replacing the tokens in the query with the values in the array.
    DECLARE @tokenName NVARCHAR(200), @tokenValue NVARCHAR(MAX);
    DECLARE token_cur CURSOR LOCAL FAST_FORWARD FOR -- only inside stored proc execution and FF cursor lowest overhead cost
        SELECT TokenName, TokenValue
        FROM @Tokens
        ORDER BY LEN(TokenName) DESC; -- avoids $id, $id2 bug, basically longest variable name goes first

    -- iterate by each token and replace any $tokens or ${tokens}
    OPEN token_cur;
    FETCH NEXT FROM token_cur INTO @tokenName, @tokenValue;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @query = REPLACE(@query, N'$'  + @tokenName, ISNULL(@tokenValue, N''));
        SET @query = REPLACE(@query, N'${' + @tokenName + N'}', ISNULL(@tokenValue, N''));
        FETCH NEXT FROM token_cur INTO @tokenName, @tokenValue;
    END

    -- close and deallocate to release resources
    CLOSE token_cur;
    DEALLOCATE token_cur;

    -- return single row with SQL updated
    SELECT @query AS resolved_source_query;
END;

GO

