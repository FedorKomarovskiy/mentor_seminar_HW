import trino
import pandas as pd
from typing import List, Dict, Any, Optional
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_HOST = os.getenv('TRINO_HOST', 'trino')
DEFAULT_PORT = int(os.getenv('TRINO_PORT', '8080'))
DEFAULT_USER = os.getenv('TRINO_USER', 'trino')
DEFAULT_CATALOG = 'system'
DEFAULT_SCHEMA = 'information_schema'


def create_trino_connection(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    user: str = DEFAULT_USER,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA
) -> trino.dbapi.Connection:
    """
    Establish a connection to Trino coordinator.
    
    Args:
        host: Trino coordinator hostname
        port: Trino coordinator port
        user: Username for authentication
        catalog: Default catalog to connect to
        schema: Default schema to use
        
    Returns:
        Active Trino database connection
    """
    try:
        connection = trino.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema
        )
        
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        
        if result[0] != 1:
            raise ConnectionError("Connection test failed")
            
        logger.info(f"Successfully connected to Trino at {host}:{port}")
        return connection
        
    except Exception as e:
        logger.error(f"Failed to connect to Trino: {e}")
        raise ConnectionError(f"Could not establish connection to Trino: {e}")


def test_catalog_connectivity(connection: trino.dbapi.Connection) -> Dict[str, bool]:
    """
    Test connectivity to all configured Trino catalogs.
    
    Args:
        connection: Active Trino connection
        
    Returns:
        Dictionary mapping catalog names to connectivity status
    """
    cursor = connection.cursor()
    catalog_status = {}
    
    try:
        cursor.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cursor.fetchall()]
        
        for catalog in catalogs:
            try:
                cursor.execute(f"SHOW SCHEMAS FROM {catalog}")
                schemas = cursor.fetchall()
                catalog_status[catalog] = len(schemas) > 0
                logger.info(f"Catalog '{catalog}': {'OK' if catalog_status[catalog] else 'FAILED'}")
            except Exception as e:
                catalog_status[catalog] = False
                logger.warning(f"Catalog '{catalog}' connectivity failed: {e}")
                
        return catalog_status
        
    except Exception as e:
        logger.error(f"Failed to test catalog connectivity: {e}")
        raise RuntimeError(f"Could not test catalog connectivity: {e}")
    finally:
        cursor.close()


def execute_sql_query(
    connection: trino.dbapi.Connection,
    query: str,
    parameters: Optional[List[Any]] = None
) -> pd.DataFrame:
    """
    Execute SQL query and return results as pandas DataFrame.
    
    Args:
        connection: Active Trino connection
        query: SQL query to execute
        parameters: Optional query parameters for parameterized queries
        
    Returns:
        Query results as pandas DataFrame
    """
    cursor = connection.cursor()
    
    try:
        if parameters:
            cursor.execute(query, parameters)
        else:
            cursor.execute(query)
            
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        
        logger.info(f"Query executed successfully, returned {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise RuntimeError(f"Could not execute query: {e}")
    finally:
        cursor.close()


def get_catalog_tables(
    connection: trino.dbapi.Connection,
    catalog: str,
    schema: str = None
) -> pd.DataFrame:
    """
    Get list of tables in a specific catalog and schema.
    
    Args:
        connection: Active Trino connection
        catalog: Catalog name to query
        schema: Optional schema name, if None shows all schemas
        
    Returns:
        DataFrame with table information
    """
    if schema:
        query = f"SHOW TABLES FROM {catalog}.{schema}"
    else:
        query = f"""
        SELECT table_schema, table_name, table_type
        FROM {catalog}.information_schema.tables
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
        """
    
    return execute_sql_query(connection, query)


def test_data_access(connection: trino.dbapi.Connection) -> Dict[str, int]:
    """
    Test data access across all configured catalogs.
    
    Args:
        connection: Active Trino connection
        
    Returns:
        Dictionary mapping table identifiers to row counts
    """
    test_queries = {
        "postgresql.public.trn_customers": "SELECT COUNT(*) FROM postgresql.public.trn_customers",
        "postgresql.public.trn_orders": "SELECT COUNT(*) FROM postgresql.public.trn_orders",
        "mysql.demo_db.trn_payments": "SELECT COUNT(*) FROM mysql.demo_db.trn_payments"
    }
    
    results = {}
    
    for table_name, query in test_queries.items():
        try:
            df = execute_sql_query(connection, query)
            count = df.iloc[0, 0] if not df.empty else 0
            results[table_name] = count
            logger.info(f"Table {table_name}: {count} rows")
        except Exception as e:
            logger.warning(f"Failed to access {table_name}: {e}")
            results[table_name] = -1
            
    return results


def close_connection(connection: trino.dbapi.Connection) -> None:
    """
    Safely close Trino connection.
    
    Args:
        connection: Trino connection to close
    """
    try:
        connection.close()
        logger.info("Trino connection closed successfully")
    except Exception as e:
        logger.warning(f"Error closing connection: {e}")