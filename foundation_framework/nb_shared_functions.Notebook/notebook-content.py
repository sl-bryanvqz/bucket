# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import sys
import logging

import urllib.parse
from sqlalchemy import create_engine, text
from typing import Dict, Any, Optional, List

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define a function for logging
def setup_logging():
    '''
    Set up logging configuration for Fabric notebooks.
    '''
    # Create formatter
    FORMAT = "%(asctime)s UTC - %(levelname)s - %(message)s (%(name)s)"
    formatter = logging.Formatter(fmt=FORMAT)

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)

    # Check if handlers exist, if not add console handler
    if not root_logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)
    else:
        # Apply formatter to existing handlers
        for handler in root_logger.handlers:
            handler.setFormatter(formatter)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class DirectoryMaintenance:

    def __init__(self):
        pass


    def _ls(self, path):
        try:
            return notebookutils.fs.ls(path)
        except Exception:
            return []


    def _is_dir(self, entry):
        return getattr(entry, "isDir", False)


    def _depth(self, path: str) -> int:
        return path.strip("/").count("/")

    
    def delete_empty_directories(self, root: str, protect_root: bool = True) -> None:
        """
        Delete empty directories under `root`, deepest first.
        - `protect_root=True` prevents deleting the root itself.
        """
        # 1) collect all subdirectories
        stack = [root]
        directories = set()
        while stack:
            current = stack.pop()
            entries = self._ls(current)
            for entry in entries:
                if self._is_dir(entry):
                    directories.add(entry.path)
                    stack.append(entry.path)

        # 2) include the root for evaluation (we may skip deleting it)
        directories = list(directories | {root})
        # deepest first
        directories.sort(key=self._depth, reverse=True)

        # 3) delete empties
        for directory in directories:
            if protect_root and directory.rstrip("/") == root.rstrip("/"):
                continue
            if len(self._ls(directory)) == 0:
                try:
                    notebookutils.fs.rm(directory, True)  # recurrentsive delete of empty dir marker
                    file_path = f"Files/{directory.split('/Files/')[1]}"
                    logger.info(f"Deleted empty dir: {file_path}")
                except Exception as e:
                    # harmless if raced with another task or if the dir just got new files
                    pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
from typing import Dict


def get_metadata_control_db_connection_info(akv_uri, tenant_id_secret_name, client_id_secret_name, client_secret_name, server, database, schema_name: str = "dbo") -> Dict[str, str]:
    """
    Retrieve metadata control database connection information from Azure Key Vault.
    
    Args:
        schema_name: Default schema name (defaults to 'dbo')
        
    Returns:
        Dictionary with connection parameters
        
    Raises:
        ValueError: If required secrets are missing or empty
    """
    try:
        # As of October 2025, SPN's cannot access Variable Libraries through Notebooks. Thus, this section is commented out. In the event that 
        # Microsoft fixes this issue, the code can be re-instated

        # Get variable libraries
        # vl_authentication = notebookutils.variableLibrary.getLibrary('vl_authentication')
        # vl_guids = notebookutils.variableLibrary.getLibrary('vl_guids')

        # Extract configuration values
        # akv_uri = vl_authentication.akv_uri
        # tenant_id_secret_name = vl_authentication.tenant_id_secret_name
        # client_id_secret_name = vl_authentication.client_id_secret_name
        # client_secret_name = vl_authentication.client_secret_name

        # Retrieve secrets from Key Vault
        tenant_id = notebookutils.credentials.getSecret(akv_uri, tenant_id_secret_name)
        client_id = notebookutils.credentials.getSecret(akv_uri, client_id_secret_name)
        client_secret = notebookutils.credentials.getSecret(akv_uri, client_secret_name)

        # Validate secrets were retrieved
        if not all([tenant_id, client_id, client_secret]):
            raise ValueError("One or more secrets could not be retrieved from Key Vault")

        # server = vl_guids.metadata_control_db_server
        # database = vl_guids.metadata_control_db_database

        return {
            #'tenant_id': tenant_id,
            'client_id': client_id,
            'client_secret': client_secret,
            'server': server,
            'database': database,
            'schema_name': schema_name
        }
        
    except Exception as e:
        logging.error(f"Failed to retrieve database connection information: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class SQLDatabase:
    """
    A class for managing SQL Server database connections and operations using SQLAlchemy.
    """
    
    def __init__(self, server: str, database: str, client_id: str, client_secret: str, schema_name: str = "dbo"):
        """
        Initialize the database connection.
        
        Args:
            server: SQL Server instance
            database: Database name
            client_id: Service principal client ID
            client_secret: Service principal client secret
            schema_name: Default schema name (defaults to 'dbo')
        """
        self.server = server
        self.database = database
        self.client_id = client_id
        self.client_secret = client_secret
        self.schema_name = schema_name
        self.engine = None
        self._create_engine()
    
    def _create_engine(self):
        """Create the SQLAlchemy engine with the connection string."""
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.client_id};"
            f"PWD={self.client_secret};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"DefaultSchema={self.schema_name};"
            f"Encrypt=yes;"
        )
        
        # URL encode the connection string      
        params = urllib.parse.quote_plus(connection_string)
        self.engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """
        Execute a SELECT query and return results as a list of dictionaries.
        
        Args:
            query: SQL query string
            params: Optional parameters for the query
            
        Returns:
            List of dictionaries representing the result rows
        """
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                # Convert result to list of dictionaries
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]
                
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            raise
    
    def execute_non_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Execute a non-SELECT query (INSERT, UPDATE, DELETE, etc.) and return affected row count.
        
        Args:
            query: SQL query string
            params: Optional parameters for the query
            
        Returns:
            Number of affected rows
        """
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                conn.commit()
                return result.rowcount
                
        except Exception as e:
            logging.error(f"Error executing non-query: {e}")
            raise
    
    def execute_stored_procedure(self, proc_name: str, show_query: bool = False, return_results: bool = False, **params) -> Any:
        """
        Execute a stored procedure with optional parameters.
        
        Args:
            proc_name: Name of the stored procedure
            return_results: Whether to return results (True) or just row count (False)
            **params: Keyword arguments for procedure parameters
            
        Returns:
            List of dictionaries if return_results=True, otherwise row count
        """
        exec_statement = self._build_exec_statement(proc_name, **params)
        if show_query:
            print(exec_statement)
        if return_results:
            return self.execute_query(exec_statement)
        else:
            return self.execute_non_query(exec_statement)
    
    def _build_exec_statement(self, proc_name: str, **params) -> str:
        """
        Build an EXEC statement for a stored procedure with parameters.
        
        Args:
            proc_name: Name of the stored procedure
            **params: Keyword arguments for procedure parameters
            
        Returns:
            Formatted EXEC statement
        """
        param_strs = []
        for key, value in params.items():
            if value is not None:
                if isinstance(value, str):
                    param_strs.append(f"@{key}='{value}'")
                else:
                    param_strs.append(f"@{key}={value}")
        
        if param_strs:
            return f"EXEC {proc_name} " + ", ".join(param_strs)
        else:
            return f"EXEC {proc_name}"
    
    def test_connection(self) -> bool:
        """
        Test the database connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logging.error(f"Connection test failed: {e}")
            return False
    
    def close(self):
        """Close the database engine."""
        if self.engine:
            self.engine.dispose()


    @classmethod
    def from_metadata_control_db(cls, akv_uri, tenant_id_secret_name, client_id_secret_name, client_secret, server, database, schema_name: str = "meta"):
        """
        Create SQLDatabase instance for metadata control database using Key Vault credentials.
        
        Args:
            schema_name: Schema name for the metadata database (defaults to 'meta')
            
        Returns:
            SQLDatabase instance connected to metadata control database
        """
        connection_info = get_metadata_control_db_connection_info(akv_uri, tenant_id_secret_name, client_id_secret_name, client_secret, server, database, schema_name)
        return cls(**connection_info)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
