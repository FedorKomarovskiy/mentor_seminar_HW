import pandas as pd
import trino
from typing import Dict, Any, Optional
import logging
from datetime import datetime, date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_FILL_VALUE = 0.0
DEFAULT_BATCH_SIZE = 1000


def aggregate_daily_orders(connection: trino.dbapi.Connection) -> pd.DataFrame:
    """
    Aggregate daily orders data from PostgreSQL.
    
    Args:
        connection: Active Trino connection
        
    Returns:
        DataFrame with daily order aggregations containing:
        - dt: Date of orders
        - revenue: Total revenue for the day
        - orders_cnt: Number of orders for the day
    """
    query = """
    SELECT 
        DATE(order_ts) as dt,
        SUM(total_amount) as revenue,
        COUNT(*) as orders_cnt
    FROM postgresql.public.trn_orders
    GROUP BY DATE(order_ts)
    ORDER BY dt
    """
    
    cursor = connection.cursor()
    
    try:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        
        df['dt'] = pd.to_datetime(df['dt']).dt.date
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
        df['orders_cnt'] = pd.to_numeric(df['orders_cnt'], errors='coerce')
        
        logger.info(f"Aggregated {len(df)} days of order data")
        return df
        
    except Exception as e:
        logger.error(f"Daily orders aggregation failed: {e}")
        raise RuntimeError(f"Could not aggregate daily orders: {e}")
    finally:
        cursor.close()


def aggregate_daily_payments(connection: trino.dbapi.Connection) -> pd.DataFrame:
    """
    Aggregate daily payments data from MySQL.
    
    Args:
        connection: Active Trino connection
        
    Returns:
        DataFrame with daily payment aggregations containing:
        - dt: Date of payments
        - paid_amount: Total amount paid for the day
        - payments_cnt: Number of payments for the day
    """
    query = """
    SELECT 
        DATE(paid_at) as dt,
        SUM(amount) as paid_amount,
        COUNT(*) as payments_cnt
    FROM mysql.demo_db.trn_payments
    GROUP BY DATE(paid_at)
    ORDER BY dt
    """
    
    cursor = connection.cursor()
    
    try:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        
        df['dt'] = pd.to_datetime(df['dt']).dt.date
        df['paid_amount'] = pd.to_numeric(df['paid_amount'], errors='coerce')
        df['payments_cnt'] = pd.to_numeric(df['payments_cnt'], errors='coerce')
        
        logger.info(f"Aggregated {len(df)} days of payment data")
        return df
        
    except Exception as e:
        logger.error(f"Daily payments aggregation failed: {e}")
        raise RuntimeError(f"Could not aggregate daily payments: {e}")
    finally:
        cursor.close()


def merge_dataframes_with_fillna(
    orders_df: pd.DataFrame,
    payments_df: pd.DataFrame,
    merge_key: str = 'dt',
    fill_value: float = DEFAULT_FILL_VALUE
) -> pd.DataFrame:
    """
    Merge orders and payments DataFrames with proper handling of missing values.
    
    Args:
        orders_df: DataFrame with daily orders data
        payments_df: DataFrame with daily payments data
        merge_key: Column name to merge on
        fill_value: Value to fill missing data with
        
    Returns:
        Merged DataFrame with all columns and missing values filled
    """
    if merge_key not in orders_df.columns:
        raise ValueError(f"Merge key '{merge_key}' not found in orders DataFrame")
    if merge_key not in payments_df.columns:
        raise ValueError(f"Merge key '{merge_key}' not found in payments DataFrame")
    
    try:
        merged_df = pd.merge(
            orders_df,
            payments_df,
            on=merge_key,
            how='outer'
        )
        
        numeric_columns = merged_df.select_dtypes(include=['number']).columns
        merged_df[numeric_columns] = merged_df[numeric_columns].fillna(fill_value)
        
        merged_df = merged_df.sort_values(merge_key).reset_index(drop=True)
        
        logger.info(f"Merged DataFrames: {len(merged_df)} total rows")
        logger.info(f"Filled missing values with {fill_value}")
        
        return merged_df
        
    except Exception as e:
        logger.error(f"DataFrame merge failed: {e}")
        raise RuntimeError(f"Could not merge DataFrames: {e}")


def calculate_payment_coverage(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate payment coverage metric for the merged DataFrame.
    
    Args:
        merged_df: DataFrame with both orders and payments data
        
    Returns:
        DataFrame with added payment_coverage column
    """
    required_columns = ['revenue', 'paid_amount']
    missing_columns = [col for col in required_columns if col not in merged_df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    try:
        result_df = merged_df.copy()
        
        result_df['payment_coverage'] = result_df.apply(
            lambda row: (
                row['paid_amount'] / row['revenue'] 
                if row['revenue'] > 0 
                else 0.0
            ),
            axis=1
        )
        
        result_df['payment_coverage'] = result_df['payment_coverage'].clip(upper=1.0)
        
        logger.info("Payment coverage calculated successfully")
        logger.info(f"Average payment coverage: {result_df['payment_coverage'].mean():.2%}")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Payment coverage calculation failed: {e}")
        raise RuntimeError(f"Could not calculate payment coverage: {e}")


def create_final_analytics_dataframe(connection: trino.dbapi.Connection) -> pd.DataFrame:
    """
    Create the final analytics DataFrame by aggregating and merging all data.
    
    Args:
        connection: Active Trino connection
        
    Returns:
        Final DataFrame with all analytics data:
        - dt: Date
        - revenue: Daily revenue from orders
        - orders_cnt: Number of orders per day
        - payments_cnt: Number of payments per day
        - paid_amount: Total amount paid per day
        - payment_coverage: Payment coverage ratio (paid_amount/revenue)
    """
    try:
        logger.info("Step 1: Aggregating daily orders...")
        orders_df = aggregate_daily_orders(connection)
        
        logger.info("Step 2: Aggregating daily payments...")
        payments_df = aggregate_daily_payments(connection)
        
        logger.info("Step 3: Merging DataFrames...")
        merged_df = merge_dataframes_with_fillna(orders_df, payments_df)
        
        logger.info("Step 4: Calculating payment coverage...")
        final_df = calculate_payment_coverage(merged_df)
        
        column_order = ['dt', 'revenue', 'orders_cnt', 'payments_cnt', 'paid_amount', 'payment_coverage']
        final_df = final_df[column_order]
        
        logger.info(f"Final analytics DataFrame created with {len(final_df)} rows")
        logger.info(f"Date range: {final_df['dt'].min()} to {final_df['dt'].max()}")
        
        return final_df
        
    except Exception as e:
        logger.error(f"Failed to create final analytics DataFrame: {e}")
        raise RuntimeError(f"Could not create final analytics DataFrame: {e}")


def get_analytics_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate summary statistics for the analytics DataFrame.
    
    Args:
        df: Analytics DataFrame
        
    Returns:
        Dictionary with summary statistics
    """
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    required_columns = ['revenue', 'orders_cnt', 'payments_cnt', 'paid_amount', 'payment_coverage']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    try:
        summary = {
            'total_days': len(df),
            'date_range': {
                'start': df['dt'].min(),
                'end': df['dt'].max()
            },
            'revenue': {
                'total': float(df['revenue'].sum()),
                'average_daily': float(df['revenue'].mean()),
                'max_daily': float(df['revenue'].max()),
                'min_daily': float(df['revenue'].min())
            },
            'orders': {
                'total': int(df['orders_cnt'].sum()),
                'average_daily': float(df['orders_cnt'].mean()),
                'max_daily': int(df['orders_cnt'].max()),
                'min_daily': int(df['orders_cnt'].min())
            },
            'payments': {
                'total': int(df['payments_cnt'].sum()),
                'total_amount': float(df['paid_amount'].sum()),
                'average_daily_count': float(df['payments_cnt'].mean()),
                'average_daily_amount': float(df['paid_amount'].mean())
            },
            'payment_coverage': {
                'average': float(df['payment_coverage'].mean()),
                'max': float(df['payment_coverage'].max()),
                'min': float(df['payment_coverage'].min()),
                'days_with_full_coverage': int((df['payment_coverage'] >= 1.0).sum())
            }
        }
        
        logger.info("Analytics summary generated successfully")
        return summary
        
    except Exception as e:
        logger.error(f"Failed to generate analytics summary: {e}")
        raise RuntimeError(f"Could not generate analytics summary: {e}")