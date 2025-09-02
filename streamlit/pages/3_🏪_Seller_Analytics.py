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
    page_title="Seller Analytics - Olist Dashboard",
    page_icon="🏪",
    layout="wide"
)

st.title("🏪 Seller Analytics")
st.markdown("---")

# Helper function to build table reference
def get_table_ref(table_name):
    return f"`{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{table_name}`"

# Fast SQL-based analytics functions
@st.cache_data(ttl=3600)
def get_table_columns():
    """Get column names from seller analytics table"""
    query = f"""
    SELECT column_name, data_type
    FROM `{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{ANALYTICS_TABLES["seller"]}'
    ORDER BY ordinal_position
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_seller_sample():
    """Get sample data to understand the structure"""
    query = f"""
    SELECT *
    FROM {get_table_ref(ANALYTICS_TABLES["seller"])}
    LIMIT 5
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_seller_metrics():
    """Get key seller metrics using SQL aggregation with actual columns"""
    query = f"""
    SELECT 
        COUNT(DISTINCT seller_id) as total_sellers,
        COUNT(*) as total_records,
        ROUND(AVG(total_revenue), 2) as avg_revenue_per_seller,
        ROUND(AVG(total_orders), 1) as avg_orders_per_seller,
        ROUND(AVG(revenue_per_order), 2) as avg_revenue_per_order,
        ROUND(AVG(days_active), 0) as avg_days_active,
        ROUND(SUM(total_revenue), 2) as total_marketplace_revenue,
        ROUND(AVG(total_items_sold), 1) as avg_items_per_seller,
        ROUND(AVG(unique_customers), 1) as avg_customers_per_seller,
        ROUND(AVG(avg_review_score), 2) as avg_seller_rating
    FROM {get_table_ref(ANALYTICS_TABLES["seller"])}
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_seller_performance_tiers():
    """Get seller performance analysis using actual performance_tier column"""
    query = f"""
    SELECT 
        performance_tier,
        COUNT(DISTINCT seller_id) as sellers,
        ROUND(AVG(total_revenue), 2) as avg_revenue,
        ROUND(AVG(total_orders), 1) as avg_orders,
        ROUND(AVG(revenue_per_order), 2) as avg_revenue_per_order,
        ROUND(SUM(total_revenue), 2) as total_tier_revenue,
        ROUND(AVG(avg_review_score), 2) as avg_rating
    FROM {get_table_ref(ANALYTICS_TABLES["seller"])}
    WHERE performance_tier IS NOT NULL
    GROUP BY performance_tier
    ORDER BY AVG(total_revenue) DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_geographic_seller_distribution():
    """Get seller geographic distribution using actual columns"""
    query = f"""
    SELECT 
        seller_state,
        COUNT(DISTINCT seller_id) as seller_count,
        ROUND(AVG(total_revenue), 2) as avg_revenue_per_seller,
        ROUND(SUM(total_revenue), 2) as total_state_revenue,
        ROUND(AVG(total_orders), 1) as avg_orders_per_seller,
        ROUND(AVG(cross_state_sales_pct), 1) as avg_cross_state_sales_pct
    FROM {get_table_ref(ANALYTICS_TABLES["seller"])}
    WHERE seller_state IS NOT NULL
    GROUP BY seller_state
    ORDER BY seller_count DESC
    LIMIT 15
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_seller_activity_analysis():
    """Get seller activity analysis using activity_level column"""
    query = f"""
    SELECT 
        activity_level,
        COUNT(DISTINCT seller_id) as sellers,
        ROUND(AVG(total_revenue), 2) as avg_revenue,
        ROUND(AVG(total_orders), 1) as avg_orders,
        ROUND(AVG(days_since_last_sale), 0) as avg_days_since_last_sale,
        ROUND(AVG(operational_consistency), 2) as avg_consistency
    FROM {get_table_ref(ANALYTICS_TABLES["seller"])}
    WHERE activity_level IS NOT NULL
    GROUP BY activity_level
    ORDER BY AVG(total_revenue) DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_seller_segments():
    """Get seller segmentation analysis"""
    query = f"""
    SELECT 
        seller_segment,
        COUNT(DISTINCT seller_id) as sellers,
        ROUND(AVG(total_revenue), 2) as avg_revenue,
        ROUND(AVG(total_orders), 1) as avg_orders,
        ROUND(AVG(unique_customers), 1) as avg_customers,
        ROUND(AVG(avg_review_score), 2) as avg_rating
    FROM {get_table_ref(ANALYTICS_TABLES["seller"])}
    WHERE seller_segment IS NOT NULL
    GROUP BY seller_segment
    ORDER BY sellers DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_top_performing_sellers():
    """Get top performing sellers with actual columns"""
    query = f"""
    SELECT 
        seller_id,
        seller_state,
        seller_segment,
        total_revenue,
        total_orders,
        total_items_sold,
        revenue_per_order,
        unique_customers,
        avg_review_score,
        days_active,
        performance_tier
    FROM {get_table_ref(ANALYTICS_TABLES["seller"])}
    ORDER BY total_revenue DESC
    LIMIT 20
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_quality_analysis():
    """Get seller quality analysis"""
    query = f"""
    SELECT 
        quality_tier,
        COUNT(DISTINCT seller_id) as sellers,
        ROUND(AVG(avg_review_score), 2) as avg_rating,
        ROUND(AVG(positive_review_rate_pct), 1) as avg_positive_rate,
        ROUND(AVG(total_revenue), 2) as avg_revenue,
        ROUND(AVG(customer_repeat_rate), 2) as avg_repeat_rate
    FROM {get_table_ref(ANALYTICS_TABLES["seller"])}
    WHERE quality_tier IS NOT NULL
    GROUP BY quality_tier
    ORDER BY AVG(avg_review_score) DESC
    """
    return execute_custom_query(query)

# Main content
try:
    # First, let's examine the table structure
    st.subheader("🔍 Table Structure Analysis")
    
    with st.expander("📋 Seller Analytics Table Columns"):
        with st.spinner("Loading table structure..."):
            columns_df = get_table_columns()
        
        if not columns_df.empty:
            st.dataframe(columns_df, use_container_width=True)
        else:
            st.warning("Could not retrieve column information")
    
    with st.expander("📊 Sample Data"):
        with st.spinner("Loading sample data..."):
            sample_df = get_seller_sample()
        
        if not sample_df.empty:
            st.dataframe(sample_df, use_container_width=True)
            st.info(f"Available columns: {', '.join(sample_df.columns)}")
        else:
            st.warning("Could not retrieve sample data")
    
    # Load basic metrics
    with st.spinner("Loading seller metrics..."):
        metrics_df = get_seller_metrics()
    
    if metrics_df.empty:
        st.warning("No seller data available. Please check your database connection.")
        st.stop()
    
    metrics = metrics_df.iloc[0]
    
    # Seller Overview Metrics - Enhanced with actual data
    st.subheader("🏪 Seller Performance Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Sellers",
            value=f"{int(metrics['total_sellers']):,}",
            help="Total number of active sellers"
        )
    
    with col2:
        st.metric(
            label="Avg Revenue/Seller", 
            value=f"R$ {metrics['avg_revenue_per_seller']:,.2f}",
            help="Average revenue per seller"
        )
    
    with col3:
        st.metric(
            label="Avg Orders/Seller",
            value=f"{metrics['avg_orders_per_seller']:.1f}",
            help="Average number of orders per seller"
        )
    
    with col4:
        st.metric(
            label="Avg Revenue/Order",
            value=f"R$ {metrics['avg_revenue_per_order']:,.2f}",
            help="Average revenue per order"
        )
    
    # Additional metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Marketplace Revenue",
            value=f"R$ {metrics['total_marketplace_revenue']:,.2f}",
            help="Total revenue generated by all sellers"
        )
    
    with col2:
        st.metric(
            label="Avg Days Active",
            value=f"{int(metrics['avg_days_active'])} days",
            help="Average seller lifetime in days"
        )
    
    with col3:
        st.metric(
            label="Avg Customers/Seller",
            value=f"{metrics['avg_customers_per_seller']:.1f}",
            help="Average unique customers per seller"
        )
    
    with col4:
        st.metric(
            label="Avg Seller Rating",
            value=f"{metrics['avg_seller_rating']:.2f} ⭐",
            help="Average seller review score"
        )
    
    st.markdown("---")
    
    # Seller Analytics Charts
    st.subheader("📊 Seller Business Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Performance Tiers")
        with st.spinner("Loading performance tiers..."):
            performance_df = get_seller_performance_tiers()
        
        if not performance_df.empty:
            fig = px.pie(performance_df, 
                        values='sellers', 
                        names='performance_tier',
                        title='Seller Distribution by Performance Tier',
                        color_discrete_sequence=COLOR_PALETTES['primary'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show performance details
            st.dataframe(performance_df.round(2), use_container_width=True)
        else:
            st.info("Performance tier data will be displayed here.")
    
    with col2:
        st.subheader("⚡ Activity Levels")
        with st.spinner("Loading activity analysis..."):
            activity_df = get_seller_activity_analysis()
        
        if not activity_df.empty:
            fig = px.bar(activity_df, 
                        x='activity_level', 
                        y='sellers',
                        title='Sellers by Activity Level',
                        labels={'sellers': 'Number of Sellers', 'activity_level': 'Activity Level'},
                        color='avg_revenue',
                        color_continuous_scale='Blues')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show activity details
            st.dataframe(activity_df.round(2), use_container_width=True)
        else:
            st.info("Activity analysis will be shown here.")
    
    # Seller Segmentation and Quality Analysis
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏷️ Seller Segments")
        with st.spinner("Loading seller segments..."):
            segments_df = get_seller_segments()
        
        if not segments_df.empty:
            fig = px.bar(segments_df, 
                        x='seller_segment', 
                        y='sellers',
                        title='Seller Distribution by Segment',
                        labels={'sellers': 'Number of Sellers', 'seller_segment': 'Seller Segment'},
                        color='avg_rating',
                        color_continuous_scale='RdYlGn')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show segment details
            st.dataframe(segments_df.round(2), use_container_width=True)
        else:
            st.info("Seller segmentation will be displayed here.")
    
    with col2:
        st.subheader("⭐ Quality Analysis")
        with st.spinner("Loading quality analysis..."):
            quality_df = get_quality_analysis()
        
        if not quality_df.empty:
            fig = px.bar(quality_df, 
                        x='quality_tier', 
                        y='sellers',
                        title='Sellers by Quality Tier',
                        labels={'sellers': 'Number of Sellers', 'quality_tier': 'Quality Tier'},
                        color='avg_rating',
                        color_continuous_scale='RdYlGn')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show quality details
            st.dataframe(quality_df.round(2), use_container_width=True)
        else:
            st.info("Quality analysis will be shown here.")
    
    # Geographic Analysis and Top Performers
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🗺️ Geographic Distribution")
        with st.spinner("Loading geographic data..."):
            geo_df = get_geographic_seller_distribution()
        
        if not geo_df.empty:
            fig = px.bar(geo_df.head(10), 
                        x='seller_state', 
                        y='seller_count',
                        title='Top 10 States by Seller Count',
                        labels={'seller_count': 'Number of Sellers', 'seller_state': 'State'},
                        color_discrete_sequence=COLOR_PALETTES['geographic'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show top states
            st.dataframe(geo_df.head(10).round(2), use_container_width=True)
        else:
            st.info("Geographic distribution will be displayed here.")
    
    with col2:
        st.subheader("🏆 Top Performing Sellers")
        with st.spinner("Loading top performers..."):
            top_sellers_df = get_top_performing_sellers()
        
        if not top_sellers_df.empty:
            # Create revenue chart
            fig = px.bar(top_sellers_df.head(10), 
                        x='seller_id', 
                        y='total_revenue',
                        title='Top 10 Sellers by Revenue',
                        labels={'total_revenue': 'Total Revenue (R$)', 'seller_id': 'Seller ID'},
                        color='avg_review_score',
                        color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show top performers table
            st.dataframe(top_sellers_df.head(10).round(2), use_container_width=True)
        else:
            st.info("Top performers will be displayed here.")
    
    # Business Insights Section
    st.markdown("---")
    st.subheader("💡 Business Insights")
    
    # Calculate insights from the data
    with st.spinner("Loading insights..."):
        performance_df = get_seller_performance_tiers()
        segments_df = get_seller_segments()
        quality_df = get_quality_analysis()
        geo_df = get_geographic_seller_distribution()
    
    if not performance_df.empty and not segments_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if not performance_df.empty:
                top_tier = performance_df.iloc[0]  # Highest performing tier
                st.success(f"""
                **Top Performance Tier**  
                {top_tier['performance_tier']}: {top_tier['sellers']} sellers averaging R$ {top_tier['avg_revenue']:,.2f}
                """)
        
        with col2:
            if not quality_df.empty:
                best_quality = quality_df.iloc[0]  # Highest quality tier
                st.info(f"""
                **Quality Excellence**  
                {best_quality['quality_tier']}: {best_quality['sellers']} sellers with {best_quality['avg_rating']:.2f}⭐ rating
                """)
        
        with col3:
            if not geo_df.empty:
                top_state = geo_df.iloc[0]
                st.warning(f"""
                **Geographic Leader**  
                {top_state['seller_state']}: {top_state['seller_count']} sellers, R$ {top_state['avg_revenue_per_seller']:,.2f} avg revenue
                """)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.info("💡 This page provides comprehensive seller analytics. Please check your database connection if you see this message.")
    
    # Fallback metrics display
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Sellers", "Loading...", "...")
    with col2:
        st.metric("Avg Revenue", "Loading...", "...")
    with col3:
        st.metric("Avg Orders", "Loading...", "...")
    with col4:
        st.metric("Connection", "✅ Active", "OK")
