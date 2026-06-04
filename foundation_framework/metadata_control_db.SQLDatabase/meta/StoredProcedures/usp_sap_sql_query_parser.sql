CREATE    PROCEDURE [meta].[usp_sap_sql_query_parser]
  @source_settings NVARCHAR(MAX)
AS
BEGIN
  SET NOCOUNT ON;

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
    Also tolerates:
    "values": { "main_table":"...", "start_date":"..." }
  */
  DECLARE @valuesJson NVARCHAR(MAX) = JSON_QUERY(@source_settings, '$.values');

  IF @valuesJson IS NOT NULL
  BEGIN
      -- Try array-of-objects first
      INSERT INTO @Tokens (TokenName, TokenValue)
      SELECT kv.[key] AS TokenName,
              CAST(kv.[value] AS NVARCHAR(MAX)) AS TokenValue
      FROM OPENJSON(@valuesJson) AS v
      CROSS APPLY OPENJSON(v.[value]) AS kv;

      -- If nothing inserted, try object
      IF NOT EXISTS (SELECT 1 FROM @Tokens)
      BEGIN
          INSERT INTO @Tokens (TokenName, TokenValue)
          SELECT [key],
                  CAST([value] AS NVARCHAR(MAX))
          FROM OPENJSON(@valuesJson);
      END
  END;

  DECLARE @nowUtc DATETIME2(3) = SYSUTCDATETIME();
  DECLARE @nowMtn DATETIMEOFFSET(3) =
      (@nowUtc AT TIME ZONE 'UTC') AT TIME ZONE 'Mountain Standard Time';

  UPDATE t
  SET t.TokenValue =
      CASE UPPER(LTRIM(RTRIM(t.TokenValue)))

          -- Mountain time (DST-aware). Prefer ISO 8601 with offset.
          WHEN 'NOW()' THEN CONVERT(NVARCHAR(33), @nowMtn, 127)  -- 2026-01-13T14:09:19.452-07:00

          -- SAP1: dd.mm.yyyy
          WHEN 'TODAY_SAP1()'     THEN CONVERT(NVARCHAR(10), CONVERT(DATE, @nowMtn), 104)
          WHEN 'YESTERDAY_SAP1()' THEN CONVERT(NVARCHAR(10), DATEADD(DAY,-1, CONVERT(DATE, @nowMtn)), 104)
          WHEN 'TOMORROW_SAP1()'  THEN CONVERT(NVARCHAR(10), DATEADD(DAY, 1, CONVERT(DATE, @nowMtn)), 104)
          WHEN 'NOW_SAP1()'       THEN CONVERT(NVARCHAR(10), CONVERT(DATE, @nowMtn), 104) -- date-only in SAP1 format

          -- SAP2: yyyymmddHHMMSS
          WHEN 'NOW_SAP2()' THEN
              CONVERT(NVARCHAR(8), CAST(@nowMtn AS DATETIME2(0)), 112) +
              REPLACE(CONVERT(NVARCHAR(8), CAST(@nowMtn AS TIME(0)), 108), ':', '')

          WHEN 'TODAY_SAP2()'     THEN CONVERT(NVARCHAR(8), CONVERT(DATE, @nowMtn), 112)
          WHEN 'YESTERDAY_SAP2()' THEN CONVERT(NVARCHAR(8), DATEADD(DAY,-1, CONVERT(DATE, @nowMtn)), 112)
          WHEN 'TOMORROW_SAP2()'  THEN CONVERT(NVARCHAR(8), DATEADD(DAY, 1, CONVERT(DATE, @nowMtn)), 112)

          ELSE t.TokenValue
      END
  FROM @Tokens t
  WHERE t.TokenValue IS NOT NULL;

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

