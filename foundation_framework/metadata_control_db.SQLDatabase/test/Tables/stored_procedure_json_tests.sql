CREATE TABLE [test].[stored_procedure_json_tests] (
    [test_id]      BIGINT         IDENTITY (1, 1) NOT NULL,
    [sp_name]      NVARCHAR (256) NOT NULL,
    [test_name]    NVARCHAR (200) NOT NULL,
    [input_json]   NVARCHAR (MAX) NOT NULL,
    [expect_error] BIT            DEFAULT ((0)) NOT NULL,
    [assert_type]  VARCHAR (20)   NULL,
    [assert_value] NVARCHAR (MAX) NULL,
    [is_enabled]   BIT            DEFAULT ((1)) NOT NULL,
    [sort_order]   INT            DEFAULT ((100)) NOT NULL,
    [notes]        NVARCHAR (400) NULL,
    [created_utc]  DATETIME2 (3)  DEFAULT (sysutcdatetime()) NOT NULL,
    [created_by]   NVARCHAR (128) DEFAULT (suser_sname()) NOT NULL,
    [updated_utc]  DATETIME2 (3)  NULL,
    [updated_by]   NVARCHAR (128) NULL,
    PRIMARY KEY CLUSTERED ([test_id] ASC),
    CONSTRAINT [CK_test_spjson_input_isjson] CHECK (isjson([input_json])=(1))
);


GO

CREATE NONCLUSTERED INDEX [IX_test_spjson_spname_enabled]
    ON [test].[stored_procedure_json_tests]([sp_name] ASC, [is_enabled] ASC, [sort_order] ASC, [test_id] ASC);


GO

