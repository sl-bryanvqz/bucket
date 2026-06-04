
CREATE PROCEDURE integration.[usp_generate_html_table_of_failed_runs]
    @parent_run_id VARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @html NVARCHAR(MAX);

    -- Build HTML table
    SET @html = N'
    <html>
    <head>
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
                margin: 20px 0;
            }
            th {
                background-color: #d32f2f;
                color: white;
                padding: 12px;
                text-align: left;
                font-weight: bold;
            }
            td {
                border: 1px solid #ddd;
                padding: 10px;
                vertical-align: top;
            }
            tr:nth-child(even) {
                background-color: #f9f9f9;
            }
            tr:hover {
                background-color: #f5f5f5;
            }
            .error-cell {
                font-family: monospace;
                font-size: 12px;
                max-width: 500px;
                word-wrap: break-word;
            }
        </style>
    </head>
    <body>
        <h2>Failed Notebook Runs Report</h2>
        <p>Parent Run ID: ' + @parent_run_id + '</p>
        <p>The following notebooks failed during execution:</p>
        <table>
            <thead>
                <tr>
                    <th>Run ID</th>
                    <th>Task Id</th>
                    <th>Task Name</th>
                    <th>Start Date</th>
                </tr>
            </thead>
            <tbody>';

    -- Add data rows using FOR XML PATH
    SELECT @html = @html + 
        N'<tr>' +
        N'<td>' + ISNULL(CAST(run_id AS NVARCHAR(50)), 'N/A') + N'</td>' +
        N'<td>' + ISNULL(CAST(task_id AS NVARCHAR(50)), 'N/A') + N'</td>' +
        N'<td>' + ISNULL(CAST(task_name AS NVARCHAR(50)), 'N/A') + N'</td>' +
        N'<td>' + ISNULL(CAST(start_date AS NVARCHAR(50)), 'N/A') + N'</td>' +
        N'</tr>'

    FROM
        logging.task_executions
    WHERE
        parent_run_id = @parent_run_id
        AND status = 'Failed'

    -- Close HTML
    SET @html = @html + N'
            </tbody>
        </table> ' + 
        N'</p>
    </body>
    </html>';

    -- Remove new lines
    SET @html = REPLACE(REPLACE(@html, CHAR(13), ''), CHAR(10), '');
    -- Return as single row
    SELECT @html AS html_report;
END;

GO

