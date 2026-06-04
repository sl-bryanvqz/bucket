CREATE PROCEDURE [integration].[usp_get_previous_watermark_val]
   @task_id VARCHAR(50)
   ,@incremental_col_data_type VARCHAR(50)
AS
BEGIN
   DECLARE @optional_buffer INT = COALESCE((SELECT CAST(JSON_VALUE(source_settings, '$.optional_buffer') as INT) from meta.task where task_id=@task_id), 0);
   With logging_watermark_latest as 
   (
   Select W.task_id,W.watermark_value,RANK() over (partition by W.task_id order by W.ingestion_timestamp desc) as row_rank 
   from  [logging].[task_executions] E
   inner join [logging].[execution_watermark]  W 
    on W.task_id = E.task_id 
    and W.parent_run_id=E.parent_run_id
   where 
    status = 'Completed' 
    and W.task_id= @task_id
   )
   SELECT
      CASE
      WHEN LOWER(@incremental_col_data_type)  = 'datetime' and @optional_buffer = 0
         THEN COALESCE(MAX(W.watermark_value), '0001-01-01 00:00:00')
      WHEN LOWER(@incremental_col_data_type) = 'int'
         THEN COALESCE(MAX(W.watermark_value), '-1')
      WHEN LOWER(@incremental_col_data_type) = 'datetime' and @optional_buffer <> 0
         THEN COALESCE(
            FORMAT(DATEADD(DAY,-@optional_buffer,CAST(MAX(W.watermark_value) as DATETIME)), 'yyyy-MM-ddTHH:mm:ss')
         , '0001-01-01 00:00:00')
      ELSE
         COALESCE(MAX(W.watermark_value), '0001-01-01 00:00:00')
      END AS previous_watermark_val

   FROM
      logging_watermark_latest W
   WHERE
        row_rank=1
END

GO

