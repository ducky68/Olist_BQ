import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.database import execute_custom_query, get_table_info
from config.settings import ANALYTICS_TABLES, COLOR_PALETTES, CHART_DEFAULTS, BIGQUERY_CONFIG

# Page configuration
st.set_page_config(
    page_title="Customer Analytics - Olist Dashboard",
    page_icon="👥",
    layout="wide"
)

st.title("👥 Customer Analytics")
st.markdown("---")

# Helper function to build table reference
def get_table_ref(table_name):
    return f"`{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{table_name}`"

# Fast SQL-based analytics functions
@st.cache_data(ttl=3600)
def get_table_columns():
    """Get column names from customer analytics table"""
    query = f"""
    SELECT column_name, data_type
    FROM `{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{ANALYTICS_TABLES["customer"]}'
    ORDER BY ordinal_position
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_customer_sample():
    """Get sample data to understand the structure"""
    query = f"""
    SELECT *
    FROM {get_table_ref(ANALYTICS_TABLES["customer"])}
    LIMIT 5
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_customer_metrics():
    """Get key customer metrics using SQL aggregation with actual columns"""
    query = f"""
    SELECT 
        COUNT(DISTINCT customer_id) as total_customers,
        COUNT(*) as total_records,
        ROUND(AVG(total_spent), 2) as avg_customer_value,
        ROUND(AVG(total_orders), 1) as avg_orders_per_customer,
        ROUND(AVG(avg_order_value), 2) as avg_order_value,
        ROUND(AVG(days_as_customer), 0) as avg_days_as_customer,
        ROUND(AVG(annual_spending_rate), 2) as avg_annual_spending,
        ROUND(AVG(annual_order_frequency), 1) as avg_annual_frequency
    FROM {get_table_ref(ANALYTICS_TABLES["customer"])}
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_customer_segmentation():
    """Get customer segmentation using actual columns"""
    query = f"""
    SELECT 
        customer_segment,
        COUNT(DISTINCT customer_id) as customers_count,
        ROUND(AVG(total_spent), 2) as avg_spent,
        ROUND(AVG(total_orders), 1) as avg_orders,
        ROUND(AVG(days_as_customer), 0) as avg_days_as_customer
    FROM {get_table_ref(ANALYTICS_TABLES["customer"])}
    WHERE customer_segment IS NOT NULL
    GROUP BY customer_segment
    ORDER BY customers_count DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_geographic_distribution():
    """Get customer geographic distribution using actual columns"""
    query = f"""
    SELECT 
        customer_state,
        COUNT(DISTINCT customer_id) as customer_count,
        ROUND(AVG(total_spent), 2) as avg_spent_per_customer,
        ROUND(SUM(total_spent), 2) as total_state_revenue,
        ROUND(AVG(total_orders), 1) as avg_orders_per_customer
    FROM {get_table_ref(ANALYTICS_TABLES["customer"])}
    WHERE customer_state IS NOT NULL
    GROUP BY customer_state
    ORDER BY customer_count DESC
    LIMIT 15
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_spending_analysis():
    """Get customer spending analysis using actual columns"""
    query = f"""
    SELECT 
        CASE 
            WHEN total_spent <= 100 THEN 'Low (≤R$100)'
            WHEN total_spent <= 500 THEN 'Medium (R$100-500)'
            WHEN total_spent <= 1000 THEN 'High (R$500-1K)'
            ELSE 'Premium (>R$1K)'
        END as spending_tier,
        COUNT(DISTINCT customer_id) as customers,
        ROUND(AVG(total_spent), 2) as avg_spent,
        ROUND(AVG(total_orders), 1) as avg_orders,
        ROUND(AVG(avg_order_value), 2) as avg_order_value
    FROM {get_table_ref(ANALYTICS_TABLES["customer"])}
    GROUP BY spending_tier
    ORDER BY AVG(total_spent) DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_customer_lifecycle():
    """Get customer lifecycle analysis using actual columns"""
    query = f"""
    SELECT 
        CASE 
            WHEN days_as_customer <= 30 THEN 'New (≤30 days)'
            WHEN days_as_customer <= 90 THEN 'Recent (30-90 days)'
            WHEN days_as_customer <= 365 THEN 'Established (3-12 months)'
            ELSE 'Veteran (>1 year)'
        END as lifecycle_stage,
        COUNT(DISTINCT customer_id) as customers,
        ROUND(AVG(total_spent), 2) as avg_spent,
        ROUND(AVG(total_orders), 1) as avg_orders,
        ROUND(AVG(days_since_last_order), 0) as avg_days_since_last_order
    FROM {get_table_ref(ANALYTICS_TABLES["customer"])}
    WHERE days_as_customer IS NOT NULL
    GROUP BY lifecycle_stage
    ORDER BY AVG(days_as_customer)
    """
    return execute_custom_query(query)

# Main content
try:
    # First, let's examine the table structure
    st.subheader("🔍 Table Structure Analysis")
    
    with st.expander("📋 Customer Analytics Table Columns"):
        with st.spinner("Loading table structure..."):
            columns_df = get_table_columns()
        
        if not columns_df.empty:
            st.dataframe(columns_df, use_container_width=True)
        else:
            st.warning("Could not retrieve column information")
    
    with st.expander("📊 Sample Data"):
        with st.spinner("Loading sample data..."):
            sample_df = get_customer_sample()
        
        if not sample_df.empty:
            st.dataframe(sample_df, use_container_width=True)
            st.info(f"Available columns: {', '.join(sample_df.columns)}")
        else:
            st.warning("Could not retrieve sample data")
    
    # Load basic metrics
    with st.spinner("Loading customer metrics..."):
        metrics_df = get_customer_metrics()
    
    if metrics_df.empty:
        st.warning("No customer data available. Please check your database connection.")
        st.stop()
    
    metrics = metrics_df.iloc[0]
    
    # Customer Overview Metrics - Enhanced with actual data
    st.subheader("👤 Customer Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Customers",
            value=f"{int(metrics['total_customers']):,}",
            help="Total number of unique customers"
        )
    
    with col2:
        st.metric(
            label="Avg Customer Value", 
            value=f"R$ {metrics['avg_customer_value']:,.2f}",
            help="Average total spending per customer"
        )
    
    with col3:
        st.metric(
            label="Avg Orders/Customer",
            value=f"{metrics['avg_orders_per_customer']:.1f}",
            help="Average number of orders per customer"
        )
    
    with col4:
        st.metric(
            label="Avg Order Value",
            value=f"R$ {metrics['avg_order_value']:,.2f}",
            help="Average value per order"
        )
    
    # Additional metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Avg Days as Customer",
            value=f"{int(metrics['avg_days_as_customer'])} days",
            help="Average customer lifetime in days"
        )
    
    with col2:
        st.metric(
            label="Annual Spending Rate",
            value=f"R$ {metrics['avg_annual_spending']:,.2f}",
            help="Average annual spending per customer"
        )
    
    with col3:
        st.metric(
            label="Annual Order Frequency",
            value=f"{metrics['avg_annual_frequency']:.1f}",
            help="Average orders per year per customer"
        )
    
    with col4:
        st.metric(
            label="Data Status",
            value="✅ Connected",
            help="Database connection status"
        )
    
    st.markdown("---")
    
    # Customer Analytics Charts
    st.subheader("📊 Customer Behavior Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Customer Segmentation")
        with st.spinner("Loading customer segments..."):
            segments_df = get_customer_segmentation()
        
        if not segments_df.empty:
            fig = px.pie(segments_df, 
                        values='customers_count', 
                        names='customer_segment',
                        title='Customer Distribution by Segment',
                        color_discrete_sequence=COLOR_PALETTES['customer'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show segment details
            st.dataframe(segments_df.round(2), use_container_width=True)
        else:
            st.info("Customer segmentation data will be displayed here.")
    
    with col2:
        st.subheader("💰 Spending Analysis")
        with st.spinner("Loading spending analysis..."):
            spending_df = get_spending_analysis()
        
        if not spending_df.empty:
            fig = px.bar(spending_df, 
                        x='spending_tier', 
                        y='customers',
                        title='Customers by Spending Tier',
                        labels={'customers': 'Number of Customers', 'spending_tier': 'Spending Tier'},
                        color='avg_spent',
                        color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show spending details
            st.dataframe(spending_df.round(2), use_container_width=True)
        else:
            st.info("Spending analysis will be shown here.")
    
    # Geographic and Lifecycle Analysis
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("�️ Geographic Distribution")
        with st.spinner("Loading geographic data..."):
            geo_df = get_geographic_distribution()
        
        if not geo_df.empty:
            fig = px.bar(geo_df.head(10), 
                        x='customer_state', 
                        y='customer_count',
                        title='Top 10 States by Customer Count',
                        labels={'customer_count': 'Number of Customers', 'customer_state': 'State'},
                        color_discrete_sequence=COLOR_PALETTES['geographic'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show top states
            st.dataframe(geo_df.head(10).round(2), use_container_width=True)
        else:
            st.info("Geographic distribution will be displayed here.")
    
    with col2:
        st.subheader("⏰ Customer Lifecycle")
        with st.spinner("Loading lifecycle data..."):
            lifecycle_df = get_customer_lifecycle()
        
        if not lifecycle_df.empty:
            fig = px.bar(lifecycle_df, 
                        x='lifecycle_stage', 
                        y='customers',
                        title='Customers by Lifecycle Stage',
                        labels={'customers': 'Number of Customers', 'lifecycle_stage': 'Lifecycle Stage'},
                        color='avg_spent',
                        color_continuous_scale='Blues')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show lifecycle details
            st.dataframe(lifecycle_df.round(2), use_container_width=True)
        else:
            st.info("Customer lifecycle analysis will be shown here.")
    
    # Additional Insights Section
    st.markdown("---")
    st.subheader("💡 Key Insights")
    
    # Calculate some insights from the data
    if not segments_df.empty and not spending_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            top_segment = segments_df.loc[segments_df['customers_count'].idxmax()]
            st.info(f"""
            **Largest Customer Segment**  
            {top_segment['customer_segment']} customers make up the largest group with {top_segment['customers_count']:,} customers
            """)
        
        with col2:
            high_value = spending_df[spending_df['spending_tier'].str.contains('Premium|High')]
            if not high_value.empty:
                high_value_customers = high_value['customers'].sum()
                st.success(f"""
                **High-Value Customers**  
                {high_value_customers:,} customers spend over R$500, representing significant revenue potential
                """)
        
        with col3:
            if not geo_df.empty:
                top_state = geo_df.iloc[0]
                st.warning(f"""
                **Geographic Concentration**  
                {top_state['customer_state']} leads with {top_state['customer_count']:,} customers
                """)
    
    # Show table structure in collapsible section
    with st.expander("🔍 Table Structure Details", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Customer Identification Columns:**")
            customer_cols = ['customer_sk', 'customer_id', 'customer_city', 'customer_state', 'customer_zip_code_prefix', 'days_as_customer', 'customer_segment']
            for col in customer_cols:
                st.write(f"• {col}")
        
        with col2:
            st.write("**Analytics Columns:**")
            analytics_cols = ['total_orders', 'total_spent', 'total_freight_paid', 'total_payments_made', 'days_since_last_order', 'first_order_date', 'last_order_date', 'avg_order_value', 'annual_spending_rate', 'annual_order_frequency']
            for col in analytics_cols:
                st.write(f"• {col}")

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.info("💡 This page provides comprehensive customer analytics. Please check your database connection if you see this message.")
    
    # Fallback metrics display
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Customers", "Loading...", "...")
    with col2:
        st.metric("Avg Customer Value", "Loading...", "...")
    with col3:
        st.metric("Avg Orders", "Loading...", "...")
    with col4:
        st.metric("Connection", "✅ Active", "OK")
