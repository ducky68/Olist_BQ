"""
BigQuery database connection utilities for Streamlit dashboard
"""

import streamlit as st
from google.cloud import bigquery
import pandas as pd
from typing import Optional
import os

from config.settings import BIGQUERY_CONFIG

@st.cache_resource
def get_bigquery_client():
    """Get cached BigQuery client instance"""
    try:
        # Initialize BigQuery client with correct location
        client = bigquery.Client(
            project=BIGQUERY_CONFIG['project_id'],
            location=BIGQUERY_CONFIG['location']
        )
        return client
    except Exception as e:
        st.error(f"Failed to connect to BigQuery: {str(e)}")
        return None

@st.cache_data(ttl=3600)  # Cache for 1 hour
def query_analytics_data(table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
    """
    Query data from analytics OBT tables
    
    Args:
        table_name: Name of the analytics OBT table (without project/dataset prefix)
        limit: Optional limit for number of rows
    
    Returns:
        DataFrame with query results
    """
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()
    
    # Use configuration from settings
    project_id = BIGQUERY_CONFIG["project_id"]
    dataset_id = BIGQUERY_CONFIG["dataset_id"]
    
    query = f"""
    SELECT * 
    FROM `{project_id}.{dataset_id}.{table_name}`
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error querying {table_name}: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def execute_custom_query(query: str) -> pd.DataFrame:
    """
    Execute a custom BigQuery SQL query
    
    Args:
        query: SQL query string
        
    Returns:
        DataFrame with query results
    """
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return pd.DataFrame()

def get_table_info(table_name: str) -> dict:
    """
    Get metadata information about a table
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary with table metadata
    """
    client = get_bigquery_client()
    if client is None:
        return {}
    
    project_id = BIGQUERY_CONFIG["project_id"]
    dataset_id = BIGQUERY_CONFIG["dataset_id"]
    
    try:
        table_ref = client.dataset(dataset_id, project=project_id).table(table_name)
        table = client.get_table(table_ref)
        
        return {
            "num_rows": table.num_rows,
            "size_mb": round(table.num_bytes / (1024 * 1024), 2),
            "created": table.created,
            "modified": table.modified,
            "description": table.description or "No description available"
        }
    except Exception as e:
        st.error(f"Error getting table info for {table_name}: {str(e)}")
        return {}
