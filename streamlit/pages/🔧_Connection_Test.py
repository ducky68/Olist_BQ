"""
Connection Test Page
Test BigQuery connection and data access
"""

import streamlit as st
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.database import get_bigquery_client, execute_custom_query
from config.settings import ANALYTICS_TABLES, BIGQUERY_CONFIG

st.set_page_config(
    page_title="Connection Test - Olist Dashboard",
    page_icon="🔧",
    layout="wide"
)

st.title("🔧 Connection Test")
st.markdown("---")

# Display configuration
st.subheader("⚙️ Current Configuration")
col1, col2 = st.columns(2)

with col1:
    st.info(f"**Project ID:** {BIGQUERY_CONFIG['project_id']}")
    st.info(f"**Dataset ID:** {BIGQUERY_CONFIG['dataset_id']}")

with col2:
    st.info(f"**Location:** {BIGQUERY_CONFIG['location']}")

# Test BigQuery connection
st.subheader("🔗 BigQuery Connection Test")

client = get_bigquery_client()
if client:
    st.success("✅ BigQuery client connection successful!")
else:
    st.error("❌ Failed to connect to BigQuery")
    st.stop()

# Test different dataset possibilities
st.subheader("� Dataset Discovery")

project_id = BIGQUERY_CONFIG['project_id']
possible_datasets = [
    "dbt_olist_analytics",
    "dbt_olist_dev_analytics", 
    "dbt_olist_dev",
    "dbt_olist"
]

st.write("Testing different possible dataset names...")

working_dataset = None
for dataset in possible_datasets:
    try:
        query = f"""
        SELECT table_name 
        FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.TABLES` 
        WHERE table_name LIKE '%analytics_obt'
        LIMIT 5
        """
        
        df = execute_custom_query(query)
        if not df.empty:
            st.success(f"✅ Found analytics tables in dataset: **{dataset}**")
            st.write(f"Tables found: {df['table_name'].tolist()}")
            working_dataset = dataset
            break
        else:
            st.info(f"📋 Dataset exists but no analytics tables: {dataset}")
    except Exception as e:
        st.warning(f"❌ Cannot access dataset: {dataset}")

if working_dataset:
    st.markdown("---")
    st.subheader(f"✅ Using Dataset: {working_dataset}")
    st.write(f"**Update your configuration to use:** `{working_dataset}`")
    
    # Test access to analytics tables
    for table_type, table_name in ANALYTICS_TABLES.items():
        try:
            query = f"SELECT COUNT(*) as row_count FROM `{project_id}.{working_dataset}.{table_name}`"
            df = execute_custom_query(query)
            if not df.empty:
                row_count = df.iloc[0]['row_count']
                st.success(f"✅ {table_name}: {row_count:,} rows")
            else:
                st.error(f"❌ {table_name}: No data returned")
        except Exception as e:
            st.error(f"❌ {table_name}: {str(e)}")
    
    if working_dataset != BIGQUERY_CONFIG['dataset_id']:
        st.warning(f"💡 **Action needed:** Update your Streamlit config to use `{working_dataset}` instead of `{BIGQUERY_CONFIG['dataset_id']}`")
        
else:
    st.error("❌ No analytics tables found in any dataset!")
    st.write("Available datasets in your project:")
    
    try:
        query = f"""
        SELECT schema_name as dataset_name
        FROM `{project_id}.INFORMATION_SCHEMA.SCHEMATA`
        WHERE schema_name LIKE '%olist%'
        ORDER BY schema_name
        """
        df = execute_custom_query(query)
        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.write("No datasets found with 'olist' in the name")
    except Exception as e:
        st.error(f"Error listing datasets: {str(e)}")
