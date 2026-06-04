CREATE VIEW meta.task_dependencies AS 
SELECT
   M1.object_name
   , M1.task_id as extract_task_id
   , M2.task_id as bronze_task_id
   , M3.task_id as silver_task_id
From meta.task M1

left JOIN meta.task M2
on M1.object_name = M2.object_name
and REPLACE(CONCAT(COALESCE(JSON_VALUE(M1.target_settings,'$.target_file_path'),NULL), COALESCE(JSON_VALUE(M1.target_settings,'$.target_file_name'),NULL)),'/', '') = REPLACE(CONCAT(COALESCE(JSON_VALUE(M2.source_settings,'$.target_file_path'),NULL), COALESCE(JSON_VALUE(M2.source_settings,'$.target_file_name'),NULL)),'/', '')

left JOIN meta.task M3
on M1.object_name = M3.object_name
and REPLACE(CONCAT(COALESCE(JSON_VALUE(M2.target_settings,'$.schema_name'),NULL), COALESCE(JSON_VALUE(M2.target_settings,'$.table_name'),NULL)),'/', '') = REPLACE(CONCAT(COALESCE(JSON_VALUE(M3.source_settings,'$.schema_name'),NULL), COALESCE(JSON_VALUE(M3.source_settings,'$.table_name'),NULL)),'/', '')

where M1.stage = 'Extract'

GO

