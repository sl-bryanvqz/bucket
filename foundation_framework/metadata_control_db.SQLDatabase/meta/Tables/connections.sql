CREATE TABLE [meta].[connections] (
    [connection_id]          INT            IDENTITY (1, 1) NOT NULL,
    [connection_name]        VARCHAR (200)  NULL,
    [uses_fabric_connection] BIT            NULL,
    [connection_settings]    VARCHAR (8000) NULL,
    [file_hash]              VARCHAR (8000) NULL
);


GO

