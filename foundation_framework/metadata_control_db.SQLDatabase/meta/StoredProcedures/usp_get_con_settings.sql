CREATE    PROCEDURE [meta].[usp_get_con_settings]
(
    @connection_name NVARCHAR(4000),
    @passphrase      NVARCHAR(256),
    @json_key NVARCHAR(4000)   -- e.g. 'session_id' OR '$.session_id' OR '$.nested.session_id'
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @settings NVARCHAR(MAX);

    SELECT @settings = connection_settings
    FROM meta.connections
    WHERE connection_name = @connection_name;
    
    -- Nothing found
    IF @settings IS NULL
    BEGIN
        SELECT CAST(NULL AS NVARCHAR(MAX)) AS connection_settings;
        RETURN;
    END

    -- Normalize to a JSONPath
    DECLARE @json_path NVARCHAR(4000) =
        CASE
            WHEN @json_key IS NULL OR LTRIM(RTRIM(@json_key)) = N'' THEN NULL
            WHEN LEFT(LTRIM(@json_key), 2) = N'$.' THEN LTRIM(@json_key)
            WHEN LEFT(LTRIM(@json_key), 1) = N'$'  THEN LTRIM(@json_key)
            ELSE N'$.' + LTRIM(@json_key)
        END;

    IF @json_path IS NULL
    BEGIN
        -- No key/path provided; return as-is
        SELECT @settings AS connection_settings;
        RETURN;
    END

    -- Get the (Base64) ciphertext at the requested path
    DECLARE @cipher_b64 NVARCHAR(MAX) = JSON_VALUE(@settings, @json_path);

    -- If missing/blank, return as-is
    IF @cipher_b64 IS NULL OR LTRIM(RTRIM(@cipher_b64)) = N''
    BEGIN
        SELECT @settings AS connection_settings;
        RETURN;
    END

    -- Base64 -> VARBINARY
    DECLARE @cipher VARBINARY(MAX) =
        CAST(N'' AS XML).value(
            'xs:base64Binary(sql:variable("@cipher_b64"))',
            'VARBINARY(MAX)'
        );
    
    -- Decrypt -> NVARCHAR (NULL if wrong passphrase / invalid cipher)
    DECLARE @plain NVARCHAR(MAX) =
        CONVERT(NVARCHAR(MAX), DECRYPTBYPASSPHRASE(@passphrase, @cipher));

    -- Put decrypted value back into the JSON at the same path
    DECLARE @out NVARCHAR(MAX) = JSON_MODIFY(@settings, @json_path, @plain);

    SELECT @out AS connection_settings;
END;

GO

