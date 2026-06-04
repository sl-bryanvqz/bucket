CREATE   PROCEDURE meta.usp_upsert_con_settings
(
    @connection_name      NVARCHAR(4000),
    @connection_settings  NVARCHAR(MAX),
    @passphrase           NVARCHAR(256)   -- supply securely at runtime
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @connection_settings IS NOT NULL AND ISJSON(@connection_settings) <> 1
        THROW 50000, 'connection_settings is not valid JSON.', 1;

    DECLARE @session_id NVARCHAR(MAX) = JSON_VALUE(@connection_settings, '$.session_id');
    DECLARE @settings_to_store NVARCHAR(MAX) = @connection_settings;

    -- Encrypt only when session_id is present and non-empty
    IF @session_id IS NOT NULL AND LTRIM(RTRIM(@session_id)) <> N''
    BEGIN
        DECLARE @cipher VARBINARY(MAX) = ENCRYPTBYPASSPHRASE(@passphrase, @session_id);

        -- VARBINARY -> Base64 string
        DECLARE @cipher_b64 NVARCHAR(MAX) =
            CAST(N'' AS XML).value(
                'xs:base64Binary(sql:variable("@cipher"))',
                'NVARCHAR(MAX)'
            );

        -- Put Base64 ciphertext back into the JSON
        SET @settings_to_store = JSON_MODIFY(@connection_settings, '$.session_id', @cipher_b64);
    END

    MERGE meta.connections AS tgt
    USING (SELECT @connection_name AS connection_name,
                  @settings_to_store AS connection_settings) AS src
      ON tgt.connection_name = src.connection_name
    WHEN MATCHED THEN
        UPDATE SET
            tgt.connection_settings = src.connection_settings,
            tgt.uses_fabric_connection = 1
    WHEN NOT MATCHED THEN
        INSERT (connection_name, uses_fabric_connection, connection_settings)
        VALUES (src.connection_name, 1, src.connection_settings);
END;

GO

