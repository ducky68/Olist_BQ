"""
Revenue Analytics Page
Comprehensive revenue analysis and financial KPIs
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.database import execute_custom_query, get_table_info
from config.settings import ANALYTICS_TABLES, COLOR_PALETTES, CHART_DEFAULTS, BIGQUERY_CONFIG

# Page configuration
st.set_page_config(
    page_title="Revenue Analytics - Olist Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Revenue Analytics")
st.markdown("---")

# Sidebar filters
st.sidebar.header("🔍 Filters")

# Helper function to build table reference
def get_table_ref(table_name):
    return f"`{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{table_name}`"

# Fast aggregated queries instead of loading all data
@st.cache_data(ttl=3600)
def get_revenue_metrics():
    """Get key revenue metrics using SQL aggregation"""
    query = f"""
    SELECT 
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT customer_id) as total_customers,
        ROUND(SUM(item_price), 2) as total_revenue,
        ROUND(AVG(item_price), 2) as avg_order_value,
        COUNT(*) as total_items
    FROM {get_table_ref(ANALYTICS_TABLES["revenue"])}
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_monthly_revenue_trend():
    """Get monthly revenue trend using SQL"""
    query = f"""
    SELECT 
        EXTRACT(YEAR FROM order_date) as year,
        EXTRACT(MONTH FROM order_date) as month,
        DATE_TRUNC(order_date, MONTH) as month_date,
        ROUND(SUM(item_price), 2) as monthly_revenue,
        COUNT(DISTINCT order_id) as monthly_orders
    FROM {get_table_ref(ANALYTICS_TABLES["revenue"])}
    WHERE order_date IS NOT NULL
    GROUP BY year, month, month_date
    ORDER BY year, month
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_top_products():
    """Get top products by revenue using SQL"""
    query = f"""
    SELECT 
        product_category_english,
        ROUND(SUM(item_price), 2) as category_revenue,
        COUNT(DISTINCT order_id) as orders_count,
        COUNT(*) as items_sold
    FROM {get_table_ref(ANALYTICS_TABLES["revenue"])}
    WHERE product_category_english IS NOT NULL
    GROUP BY product_category_english
    ORDER BY category_revenue DESC
    LIMIT 10
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_state_performance():
    """Get revenue by state using SQL"""
    query = f"""
    SELECT 
        customer_state,
        ROUND(SUM(item_price), 2) as state_revenue,
        COUNT(DISTINCT customer_id) as customers,
        COUNT(DISTINCT order_id) as orders
    FROM {get_table_ref(ANALYTICS_TABLES["revenue"])}
    WHERE customer_state IS NOT NULL
    GROUP BY customer_state
    ORDER BY state_revenue DESC
    LIMIT 15
    """
    return execute_custom_query(query)

# Main content
try:
    # Load key metrics (fast aggregated query)
    with st.spinner("Loading revenue metrics..."):
        metrics_df = get_revenue_metrics()
    
    if metrics_df.empty:
        st.warning("No revenue data available. Please check your database connection.")
        st.stop()
    
    metrics = metrics_df.iloc[0]
    
    # Revenue Overview Metrics
    st.subheader("💰 Revenue Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Revenue",
            value=f"R$ {metrics['total_revenue']:,.2f}",
            help="Total revenue across all orders"
        )
    
    with col2:
        st.metric(
            label="Average Order Value", 
            value=f"R$ {metrics['avg_order_value']:,.2f}",
            help="Average value per order"
        )
    
    with col3:
        st.metric(
            label="Total Orders",
            value=f"{metrics['total_orders']:,}",
            help="Total number of orders"
        )
    
    with col4:
        st.metric(
            label="Total Customers",
            value=f"{metrics['total_customers']:,}",
            help="Total number of unique customers"
        )
    
    st.markdown("---")
    
    # Charts section
    st.subheader("📊 Revenue Trends")
    
    # Monthly revenue trend
    with st.spinner("Loading monthly trends..."):
        monthly_df = get_monthly_revenue_trend()
    
    if not monthly_df.empty:
        fig = px.line(monthly_df, 
                     x='month_date', 
                     y='monthly_revenue',
                     title='Monthly Revenue Trend',
                     labels={'monthly_revenue': 'Revenue (R$)', 'month_date': 'Month'},
                     color_discrete_sequence=COLOR_PALETTES['revenue'])
        fig.update_layout(height=CHART_DEFAULTS['height'])
        st.plotly_chart(fig, use_container_width=True)
    
    # Additional sections
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Top Product Categories")
        with st.spinner("Loading top products..."):
            products_df = get_top_products()
        
        if not products_df.empty:
            fig = px.bar(products_df, 
                        x='category_revenue', 
                        y='product_category_english',
                        orientation='h',
                        title='Top 10 Categories by Revenue',
                        labels={'category_revenue': 'Revenue (R$)', 'product_category_english': 'Category'},
                        color_discrete_sequence=COLOR_PALETTES['revenue'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No product data available.")
    
    with col2:
        st.subheader("�️ Revenue by State")
        with st.spinner("Loading state performance..."):
            states_df = get_state_performance()
        
        if not states_df.empty:
            fig = px.bar(states_df.head(10), 
                        x='customer_state', 
                        y='state_revenue',
                        title='Top 10 States by Revenue',
                        labels={'state_revenue': 'Revenue (R$)', 'customer_state': 'State'},
                        color_discrete_sequence=COLOR_PALETTES['geographic'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No state data available.")
    
    # Table info
    st.markdown("---")
    st.subheader("ℹ️ Data Source Information")
    
    table_info = get_table_info(ANALYTICS_TABLES["revenue"])
    if table_info:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Rows", f"{table_info.get('num_rows', 'N/A'):,}")
        with col2:
            st.metric("Size", f"{table_info.get('size_mb', 'N/A')} MB")
        with col3:
            st.metric("Last Updated", str(table_info.get('modified', 'N/A'))[:10] if table_info.get('modified') else 'N/A')

except Exception as e:
    st.error(f"An error occurred while loading the revenue analytics: {str(e)}")
    st.info("💡 Please check your BigQuery connection and table permissions.")
    
    # Show sample layout as fallback
    st.subheader("📊 Sample Revenue Dashboard Layout")
    
    # Sample metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Revenue", "R$ 16,008,872.11", "12.5%")
    with col2:
        st.metric("Avg Order Value", "R$ 142.13", "5.2%")
    with col3:
        st.metric("Total Orders", "112,650", "8.7%")
    with col4:
        st.metric("Growth Rate", "15.3%", "2.1%")
