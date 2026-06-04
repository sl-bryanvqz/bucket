# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

SourceSettings = '{"SourceName": "Power BI Tenant", "Resource": "apps", "QueryParameters": "$top=500"}'
TargetSettings = '{"TargetFilePath": "pbi/apps", "TargetFileName": "apps"}'
SourceConnectionSettings = '{ "BaseUrl": "https://api.powerbi.com/v1.0/myorg/admin", "Authentication": "Service Principal", "KvUri": "https://your-key-vault-name.vault.azure.net/", "AppClientId": "your-powerbi-sp-client-id-secret", "AppTenantId": "your-powerbi-sp-tenant-id-secret", "AppSecret": "your-powerbi-sp-client-secret", "GrantType": "client_credentials", "AuthenticationBaseUrl": "https://login.microsoftonline.com", "AuthenticationResource": "https://analysis.windows.net/powerbi/api"}'
RunId = '00000000-0000-0000-0000-000000000000'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

vl_guids = notebookutils.variableLibrary.getLibrary('vl_guids')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import requests
import json
import fsspec
from datetime import datetime

SourceSettings = SourceSettings or '{}'
SourceConnectionSettings = SourceConnectionSettings or '{}'
TargetSettings = TargetSettings or '{}'

source_connection_settings = json.loads(SourceConnectionSettings)
source_settings = json.loads(SourceSettings)
target_settings = json.loads(TargetSettings)

sharepoint_url = source_connection_settings.get('SharePointUrl')
sharepoint_site_name = source_connection_settings.get('SharePointSitename')
kv_uri = source_connection_settings.get('KvUri')
app_tenant_id_name = source_connection_settings.get('AppTenantId')
app_client_id_name = source_connection_settings.get('AppClientId')
app_secret_name = source_connection_settings.get('AppSecret')
grant_type = source_connection_settings.get('GrantType')

if not all([kv_uri, app_tenant_id_name, app_client_id_name, app_secret_name]):
    raise ValueError("Missing Key Vault URI or secret names in SourceConnectionSettings.")

app_tenant_id = notebookutils.credentials.getSecret(kv_uri, app_tenant_id_name)
app_client_id = notebookutils.credentials.getSecret(kv_uri, app_client_id_name)
app_secret = notebookutils.credentials.getSecret(kv_uri, app_secret_name)

source_resource = source_settings.get('Resource')
source_query_parameters = source_settings.get('QueryParameters')

target_file_path = target_settings.get('TargetFilePath')
target_file_name = target_settings.get('TargetFileName')
target_workspace_id = vl_GUIDs.bronze_workspace_id
target_lakehouse_id = vl_GUIDs.bronze_lakehouse_id
target_abfss_path = f'abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Files'

current_date = datetime.now().strftime('%Y-%m-%d')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%run nb_utility_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

 # Initialize client
client = SharePointGraphClient(app_tenant_id, app_client_id, app_secret)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
