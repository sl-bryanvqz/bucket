CREATE VIEW meta.task_metadata AS
SELECT
    t.task_id, t.task_name, t.stage, t.source_connection_id,
    e.uses_fabric_connection AS source_uses_fabric_connection,
    e.connection_settings AS source_connection_settings,
    t.source_settings, t.target_settings, t.option_settings,
    t.template_settings, t.scheduling_settings, t.enabled
FROM
    meta.task AS t
INNER JOIN
    meta.connections AS e
    ON
        t.source_connection_id = e.connection_id;

GO

