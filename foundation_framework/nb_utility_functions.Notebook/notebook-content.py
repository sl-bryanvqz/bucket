# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ###### SharePoint

# CELL ********************

import requests
import json
import os
from pathlib import Path
import fnmatch

class SharePointGraphClient:
    def __init__(self, tenant_id, client_id, client_secret):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.base_url = "https://graph.microsoft.com/v1.0"
        
    def get_access_token(self):
        """Get access token using client credentials flow"""
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'https://graph.microsoft.com/.default',
            'grant_type': 'client_credentials'
        }
        
        response = requests.post(url, headers=headers, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data['access_token']
            return True
        else:
            print(f"Failed to get token: {response.status_code} - {response.text}")
            return False
    
    def get_headers(self):
        """Get headers with authorization"""
        if not self.access_token:
            if not self.get_access_token():
                raise Exception("Failed to obtain access token")
        
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
    def get_site_by_name(self, sharepoint_url, site_name):
        """Get site by display name"""
        site_name = site_name if "/"  in site_name else f"sites/{site_name}"
        url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_url}:/{site_name}"
        response = requests.get(url, headers=self.get_headers())
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting site: {response.status_code} - {response.text}")
            return None
    
    def get_site_drives(self, site_id):
        """Get all document libraries (drives) in a site"""
        url = f"{self.base_url}/sites/{site_id}/drives"
        response = requests.get(url, headers=self.get_headers())
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting drives: {response.status_code} - {response.text}")
            return None

    def get_site_drive_by_name(self, site_id, drive_name: str):
        drives = self.get_site_drives(site_id)['value']
        selected_drive = next(
            (drive for drive in drives if drive["name"] == drive_name),
            None
        )
        if selected_drive is None:
            drives_string = ", ".join(f"{drive['name']} ({drive['id']})" for drive in drives)
            raise DriveNotFoundException(f"List \"{source_drive_name}\" not found on \"{site_name}\". Available drives are: {drives_string}.")
        return selected_drive

    def list_files_in_drive(self, site_id, drive_id, folder_path=""):
        """List files in a specific drive/folder"""
        if folder_path:
            url = f"{self.base_url}/sites/{site_id}/drives/{drive_id}/root:/{folder_path}:/children"
        else:
            url = f"{self.base_url}/sites/{site_id}/drives/{drive_id}/root/children"
        
        response = requests.get(url, headers=self.get_headers())
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error listing files: {response.status_code} - {response.text}")
            return None
    
    def download_file(self, site_id, drive_id, item_id, local_path):
        """Download a file by item ID"""
        url = f"{self.base_url}/sites/{site_id}/drives/{drive_id}/items/{item_id}/content"
        response = requests.get(url, headers=self.get_headers(), stream=True)
        
        if response.status_code == 200:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded: {local_path}")
            return True
        else:
            print(f"Error downloading file: {response.status_code} - {response.text}")
            return False
    
    def download_file_by_path(self, site_id, drive_id, file_path, local_path):
        """Download a file by its SharePoint path"""
        url = f"{self.base_url}/sites/{site_id}/drives/{drive_id}/root:/{file_path}:/content"
        response = requests.get(url, headers=self.get_headers(), stream=True)
        
        if response.status_code == 200:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded: {local_path}")
            return True
        else:
            print(f"Error downloading file: {response.status_code} - {response.text}")
            return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sharepoint_client = SharePointGraphClient(
    tenant_id='',
    client_id='',
    client_secret=''
)

sharepoint_client.get_access_token()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

site = sharepoint_client.get_site_by_name('twodegrees1.sharepoint.com', 'powerbicommunity')
drive = sharepoint_client.get_site_drive_by_name(site['id'], drive_name='Documents')
files = sharepoint_client.list_files_in_drive(site['id'], drive['id'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
