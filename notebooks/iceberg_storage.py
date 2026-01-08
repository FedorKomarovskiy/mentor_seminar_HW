import pandas as pd
import trino
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime, date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_SCHEMA = "analytics"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_TABLE_FORMAT = "PARQUET"
DEFAULT_COMPRESSION = "SNAPPY"


def create_iceberg_schema(
    connection: trino.dbapi.Connection,
    schema_name: str = DEFAULT_SCHEMA
) -> bool:
    """
    Create a custom schema in the Iceberg catalog.
    
    Args:
        connection: Active Trino connection
        schema_name: Name of the schema to create
        
    Returns:
        True if schema was created or already exists, False otherwise
    """
    cursor = connection.cursor()
    
    try:
        cursor.execute("SHOW SCHEMAS FROM iceberg")
        existing_schemas = [row[0] for row in cursor.fetchall()]
        
        if schema_name in existing_schemas:
            logger.info(f"Schema '{schema_name}' already exists in Iceberg catalog")
            return True
        
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema_name}"
        cursor.execute(create_schema_query)
        
        logger.info(f"Schema '{schema_name}' created successfully in Iceberg catalog")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create Iceberg schema '{schema_name}': {e}")
        raise RuntimeError(f"Could not create Iceberg schema: {e}")
    finally:
        cursor.close()


def create_iceberg_table_from_dataframe(
    connection: trino.dbapi.Connection,
    df: pd.DataFrame,
    table_name: str,
    schema_name: str = DEFAULT_SCHEMA,
    drop_if_exists: bool = True
) -> bool:
    """
    Create an Iceberg table with structure matching the DataFrame schema.
    
    Args:
        connection: Active Trino connection
        df: DataFrame to base table structure on
        table_name: Name of the table to create
        schema_name: Schema name in Iceberg catalog
        drop_if_exists: Whether to drop table if it already exists
        
    Returns:
        True if table was created successfully, False otherwise
    """
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    cursor = connection.cursor()
    
    try:
        create_iceberg_schema(connection, schema_name)
        
        full_table_name = f"iceberg.{schema_name}.{table_name}"
        
        if drop_if_exists:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
                logger.info(f"Dropped existing table {full_table_name}")
            except Exception as e:
                logger.warning(f"Could not drop table {full_table_name}: {e}")
        
        def map_dtype_to_trino(dtype, column_name):
            dtype_str = str(dtype)
            
            if 'int' in dtype_str:
                return 'BIGINT'
            elif 'float' in dtype_str or 'double' in dtype_str:
                return 'DOUBLE'
            elif 'bool' in dtype_str:
                return 'BOOLEAN'
            elif 'datetime' in dtype_str or 'timestamp' in dtype_str:
                return 'TIMESTAMP(3)'
            elif 'date' in dtype_str:
                return 'DATE'
            elif column_name == 'dt':
                return 'DATE'
            else:
                return 'VARCHAR'
        
        column_definitions = []
        for column_name, dtype in df.dtypes.items():
            trino_type = map_dtype_to_trino(dtype, column_name)
            column_definitions.append(f"{column_name} {trino_type}")
        
        columns_sql = ",\\n    ".join(column_definitions)
        
        create_table_sql = f"""
        CREATE TABLE {full_table_name} (
            {columns_sql}
        ) WITH (
            format = '{DEFAULT_TABLE_FORMAT}',
            location = 's3a://iceberg/{schema_name}/{table_name}/'
        )
        """
        
        cursor.execute(create_table_sql)
        
        logger.info(f"Iceberg table {full_table_name} created successfully")
        logger.info(f"Table structure: {len(column_definitions)} columns")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create Iceberg table {table_name}: {e}")
        raise RuntimeError(f"Could not create Iceberg table: {e}")
    finally:
        cursor.close()


def insert_dataframe_to_iceberg(
    connection: trino.dbapi.Connection,
    df: pd.DataFrame,
    table_name: str,
    schema_name: str = DEFAULT_SCHEMA,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """
    Insert DataFrame data into an Iceberg table.
    
    Args:
        connection: Active Trino connection
        df: DataFrame containing data to insert
        table_name: Name of the target table
        schema_name: Schema name in Iceberg catalog
        batch_size: Number of rows to insert per batch
        
    Returns:
        Number of rows inserted
    """
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    cursor = connection.cursor()
    full_table_name = f"iceberg.{schema_name}.{table_name}"
    
    try:
        def format_value(value):
            if pd.isna(value):
                return 'NULL'
            elif isinstance(value, (date, datetime)):
                return f"DATE '{value}'"
            elif isinstance(value, str):
                escaped_value = value.replace("'", "''")
                return f"'{escaped_value}'"
            elif isinstance(value, (int, float)):
                return str(value)
            else:
                return f"'{str(value)}'"
        
        total_inserted = 0
        
        for start_idx in range(0, len(df), batch_size):
            end_idx = min(start_idx + batch_size, len(df))
            batch_df = df.iloc[start_idx:end_idx]
            
            values_list = []
            for _, row in batch_df.iterrows():
                row_values = [format_value(value) for value in row]
                values_list.append(f"({', '.join(row_values)})")
            
            values_sql = ",\\n    ".join(values_list)
            columns_sql = ", ".join(df.columns)
            
            insert_sql = f"""
            INSERT INTO {full_table_name} ({columns_sql})
            VALUES
                {values_sql}
            """
            
            cursor.execute(insert_sql)
            batch_size_actual = len(batch_df)
            total_inserted += batch_size_actual
            
            logger.info(f"Inserted batch {start_idx//batch_size + 1}: {batch_size_actual} rows")
        
        logger.info(f"Successfully inserted {total_inserted} rows into {full_table_name}")
        return total_inserted
        
    except Exception as e:
        logger.error(f"Failed to insert data into {full_table_name}: {e}")
        raise RuntimeError(f"Could not insert data into Iceberg table: {e}")
    finally:
        cursor.close()


def query_iceberg_table(
    connection: trino.dbapi.Connection,
    table_name: str,
    schema_name: str = DEFAULT_SCHEMA,
    limit: Optional[int] = None,
    where_clause: Optional[str] = None,
    order_by: Optional[str] = None
) -> pd.DataFrame:
    """
    Query data from an Iceberg table and return as DataFrame.
    
    Args:
        connection: Active Trino connection
        table_name: Name of the table to query
        schema_name: Schema name in Iceberg catalog
        limit: Optional limit on number of rows to return
        where_clause: Optional WHERE clause (without 'WHERE' keyword)
        order_by: Optional ORDER BY clause (without 'ORDER BY' keyword)
        
    Returns:
        DataFrame containing query results
    """
    cursor = connection.cursor()
    full_table_name = f"iceberg.{schema_name}.{table_name}"
    
    try:
        query_parts = [f"SELECT * FROM {full_table_name}"]
        
        if where_clause:
            query_parts.append(f"WHERE {where_clause}")
        
        if order_by:
            query_parts.append(f"ORDER BY {order_by}")
        
        if limit:
            query_parts.append(f"LIMIT {limit}")
        
        query = " ".join(query_parts)
        
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        
        for col in df.columns:
            if col == 'dt' or 'date' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col]).dt.date
                except:
                    pass
        
        logger.info(f"Queried {len(df)} rows from {full_table_name}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to query {full_table_name}: {e}")
        raise RuntimeError(f"Could not query Iceberg table: {e}")
    finally:
        cursor.close()


def verify_data_persistence(
    connection: trino.dbapi.Connection,
    original_df: pd.DataFrame,
    table_name: str,
    schema_name: str = DEFAULT_SCHEMA
) -> Dict[str, Any]:
    """
    Verify that data was correctly persisted to Iceberg table.
    
    Args:
        connection: Active Trino connection
        original_df: Original DataFrame that was inserted
        table_name: Name of the table to verify
        schema_name: Schema name in Iceberg catalog
        
    Returns:
        Dictionary with verification results
    """
    try:
        persisted_df = query_iceberg_table(connection, table_name, schema_name)
        
        verification_results = {
            'table_name': f"{schema_name}.{table_name}",
            'original_rows': len(original_df),
            'persisted_rows': len(persisted_df),
            'row_count_match': len(original_df) == len(persisted_df),
            'column_count_match': len(original_df.columns) == len(persisted_df.columns),
            'columns_match': list(original_df.columns) == list(persisted_df.columns),
            'data_types_compatible': True,
            'sample_data_match': True,
            'verification_timestamp': datetime.now()
        }
        
        try:
            for col in original_df.columns:
                if col in persisted_df.columns:
                    orig_type = str(original_df[col].dtype)
                    pers_type = str(persisted_df[col].dtype)
                    
                    if 'int' in orig_type and 'int' in pers_type:
                        continue
                    elif 'float' in orig_type and ('float' in pers_type or 'int' in pers_type):
                        continue
                    elif col == 'dt':
                        continue
                    elif orig_type != pers_type:
                        logger.warning(f"Type mismatch for column {col}: {orig_type} vs {pers_type}")
        except Exception as e:
            verification_results['data_types_compatible'] = False
            logger.warning(f"Data type verification failed: {e}")
        
        try:
            if not original_df.empty and not persisted_df.empty:
                sample_size = min(5, len(original_df), len(persisted_df))
                
                first_col = original_df.columns[0]
                orig_sample = original_df.sort_values(first_col).head(sample_size)
                pers_sample = persisted_df.sort_values(first_col).head(sample_size)
                
                numeric_cols = original_df.select_dtypes(include=['number']).columns
                for col in numeric_cols:
                    if col in pers_sample.columns:
                        orig_values = orig_sample[col].values
                        pers_values = pers_sample[col].values
                        
                        if not pd.Series(orig_values).equals(pd.Series(pers_values)):
                            if not all(abs(o - p) < 0.01 for o, p in zip(orig_values, pers_values)):
                                verification_results['sample_data_match'] = False
                                break
        except Exception as e:
            verification_results['sample_data_match'] = False
            logger.warning(f"Sample data verification failed: {e}")
        
        verification_results['verification_passed'] = all([
            verification_results['row_count_match'],
            verification_results['column_count_match'],
            verification_results['columns_match'],
            verification_results['data_types_compatible'],
            verification_results['sample_data_match']
        ])
        
        if verification_results['verification_passed']:
            logger.info(f"✅ Data persistence verification passed for {table_name}")
        else:
            logger.warning(f"⚠️ Data persistence verification had issues for {table_name}")
        
        return verification_results
        
    except Exception as e:
        logger.error(f"Data persistence verification failed: {e}")
        raise RuntimeError(f"Could not verify data persistence: {e}")


def get_iceberg_table_info(
    connection: trino.dbapi.Connection,
    table_name: str,
    schema_name: str = DEFAULT_SCHEMA
) -> Dict[str, Any]:
    """
    Get detailed information about an Iceberg table.
    
    Args:
        connection: Active Trino connection
        table_name: Name of the table
        schema_name: Schema name in Iceberg catalog
        
    Returns:
        Dictionary with table information
    """
    cursor = connection.cursor()
    full_table_name = f"iceberg.{schema_name}.{table_name}"
    
    try:
        table_info = {
            'table_name': full_table_name,
            'schema_name': schema_name,
            'table_name_short': table_name
        }
        
        cursor.execute(f"DESCRIBE {full_table_name}")
        columns_info = cursor.fetchall()
        
        table_info['columns'] = [
            {
                'name': row[0],
                'type': row[1],
                'nullable': row[2] if len(row) > 2 else None,
                'comment': row[3] if len(row) > 3 else None
            }
            for row in columns_info
        ]
        
        cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
        row_count = cursor.fetchone()[0]
        table_info['row_count'] = row_count
        
        try:
            cursor.execute(f"SHOW CREATE TABLE {full_table_name}")
            create_statement = cursor.fetchone()[0]
            table_info['create_statement'] = create_statement
        except Exception:
            table_info['create_statement'] = None
        
        logger.info(f"Retrieved information for table {full_table_name}")
        return table_info
        
    except Exception as e:
        logger.error(f"Failed to get table information for {full_table_name}: {e}")
        raise RuntimeError(f"Could not get table information: {e}")
    finally:
        cursor.close()


def save_analytics_to_iceberg(
    connection: trino.dbapi.Connection,
    df: pd.DataFrame,
    table_name: str = "daily_analytics",
    schema_name: str = DEFAULT_SCHEMA
) -> Dict[str, Any]:
    """
    Complete workflow to save analytics DataFrame to Iceberg with verification.
    
    Args:
        connection: Active Trino connection
        df: Analytics DataFrame to save
        table_name: Name of the table to create
        schema_name: Schema name in Iceberg catalog
        
    Returns:
        Dictionary with operation results and verification
    """
    try:
        logger.info(f"Starting complete Iceberg save workflow for {len(df)} rows")
        
        logger.info("Step 1: Creating Iceberg schema...")
        create_iceberg_schema(connection, schema_name)
        
        logger.info("Step 2: Creating Iceberg table...")
        create_iceberg_table_from_dataframe(connection, df, table_name, schema_name)
        
        logger.info("Step 3: Inserting data...")
        rows_inserted = insert_dataframe_to_iceberg(connection, df, table_name, schema_name)
        
        logger.info("Step 4: Verifying data persistence...")
        verification_results = verify_data_persistence(connection, df, table_name, schema_name)
        
        logger.info("Step 5: Getting table information...")
        table_info = get_iceberg_table_info(connection, table_name, schema_name)
        
        results = {
            'success': True,
            'schema_name': schema_name,
            'table_name': table_name,
            'full_table_name': f"{schema_name}.{table_name}",
            'rows_inserted': rows_inserted,
            'verification': verification_results,
            'table_info': table_info,
            'timestamp': datetime.now()
        }
        
        if verification_results['verification_passed']:
            logger.info("✅ Complete Iceberg save workflow completed successfully")
        else:
            logger.warning("⚠️ Iceberg save completed but verification had issues")
            results['success'] = False
        
        return results
        
    except Exception as e:
        logger.error(f"Iceberg save workflow failed: {e}")
        raise RuntimeError(f"Could not complete Iceberg save workflow: {e}")


def list_iceberg_tables(
    connection: trino.dbapi.Connection,
    schema_name: str = DEFAULT_SCHEMA
) -> pd.DataFrame:
    """
    List all tables in the specified Iceberg schema.
    
    Args:
        connection: Active Trino connection
        schema_name: Schema name in Iceberg catalog
        
    Returns:
        DataFrame with table information
    """
    cursor = connection.cursor()
    
    try:
        cursor.execute(f"SHOW TABLES FROM iceberg.{schema_name}")
        tables = cursor.fetchall()
        
        if not tables:
            logger.info(f"No tables found in schema iceberg.{schema_name}")
            return pd.DataFrame(columns=['table_name'])
        
        df = pd.DataFrame(tables, columns=['table_name'])
        logger.info(f"Found {len(df)} tables in schema iceberg.{schema_name}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to list tables in schema {schema_name}: {e}")
        raise RuntimeError(f"Could not list Iceberg tables: {e}")
    finally:
        cursor.close()