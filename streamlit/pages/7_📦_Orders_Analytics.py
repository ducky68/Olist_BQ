"""
Orders Analytics Page
Comprehensive order-level analysis and insights
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sys
import os
import numpy as np

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.database import execute_custom_query, get_table_info
from config.settings import ANALYTICS_TABLES, COLOR_PALETTES, CHART_DEFAULTS, BIGQUERY_CONFIG

# Page configuration
st.set_page_config(
    page_title="Orders Analytics - Olist Dashboard",
    page_icon="📦",
    layout="wide"
)

st.title("📦 Orders Analytics")
st.markdown("---")

# Sidebar filters
st.sidebar.header("🔍 Filters")

# Helper function to build table reference
def get_table_ref(table_name):
    return f"`{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{table_name}`"

# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

@st.cache_data(ttl=3600)
def get_orders_overview_metrics():
    """Get key order metrics using SQL aggregation"""
    query = f"""
    SELECT 
        COUNT(*) as total_orders,
        COUNT(DISTINCT customer_unique_id) as total_unique_customers,
        COUNT(DISTINCT customer_id) as total_customer_records,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        ROUND(SUM(total_order_value), 2) as total_revenue,
        ROUND(AVG(total_items), 2) as avg_items_per_order,
        ROUND(AVG(total_sellers), 2) as avg_sellers_per_order,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction_score,
        ROUND(AVG(avg_orders_per_customer), 2) as avg_orders_per_customer
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_order_complexity_distribution():
    """Get order complexity breakdown"""
    query = f"""
    SELECT 
        order_complexity,
        COUNT(*) as order_count,
        ROUND(AVG(total_order_value), 2) as avg_value,
        ROUND(AVG(total_items), 2) as avg_items,
        ROUND(AVG(logistics_complexity_score), 2) as avg_logistics_score
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    GROUP BY order_complexity
    ORDER BY 
        CASE order_complexity
            WHEN 'simple_order' THEN 1
            WHEN 'standard_order' THEN 2
            WHEN 'moderate_order' THEN 3
            WHEN 'complex_order' THEN 4
            WHEN 'very_complex_order' THEN 5
        END
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_order_value_tiers():
    """Get order value tier analysis"""
    query = f"""
    SELECT 
        order_value_tier,
        COUNT(*) as order_count,
        ROUND(SUM(total_order_value), 2) as total_revenue,
        ROUND(AVG(total_items), 2) as avg_items,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    GROUP BY order_value_tier
    ORDER BY 
        CASE order_value_tier
            WHEN 'premium_order' THEN 1
            WHEN 'high_value_order' THEN 2
            WHEN 'medium_value_order' THEN 3
            WHEN 'standard_order' THEN 4
            WHEN 'low_value_order' THEN 5
        END
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_delivery_performance_analysis():
    """Get delivery performance breakdown"""
    query = f"""
    SELECT 
        delivery_performance,
        COUNT(*) as order_count,
        ROUND(AVG(total_fulfillment_days), 2) as avg_fulfillment_days,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    WHERE delivery_performance IS NOT NULL
    GROUP BY delivery_performance
    ORDER BY order_count DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_monthly_orders_trend():
    """Get monthly order trends"""
    query = f"""
    SELECT 
        DATE_TRUNC(order_date, MONTH) as month_date,
        COUNT(*) as order_count,
        ROUND(SUM(total_order_value), 2) as monthly_revenue,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        ROUND(AVG(total_items), 2) as avg_items_per_order,
        ROUND(AVG(logistics_complexity_score), 2) as avg_complexity
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    WHERE order_date IS NOT NULL
    GROUP BY month_date
    ORDER BY month_date
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_geographic_orders_analysis():
    """Get orders by state and region"""
    query = f"""
    SELECT 
        customer_state,
        customer_region,
        market_tier,
        COUNT(*) as order_count,
        ROUND(SUM(total_order_value), 2) as total_revenue,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        ROUND(AVG(total_items), 2) as avg_items,
        SUM(CASE WHEN is_multi_seller_order THEN 1 ELSE 0 END) as multi_seller_orders
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    GROUP BY customer_state, customer_region, market_tier
    ORDER BY order_count DESC
    LIMIT 20
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_satisfaction_vs_complexity():
    """Analyze satisfaction vs order complexity"""
    query = f"""
    SELECT 
        order_complexity,
        satisfaction_level,
        COUNT(*) as order_count,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        ROUND(AVG(total_fulfillment_days), 2) as avg_fulfillment_days
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    WHERE satisfaction_level != 'no_feedback'
    GROUP BY order_complexity, satisfaction_level
    ORDER BY order_complexity, satisfaction_level
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_payment_behavior_analysis():
    """Analyze payment behavior patterns"""
    query = f"""
    SELECT 
        payment_behavior_type,
        COUNT(*) as order_count,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        ROUND(AVG(max_installments), 2) as avg_installments,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    GROUP BY payment_behavior_type
    ORDER BY order_count DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_customer_behavior_analysis():
    """Analyze customer behavior patterns using customer_unique_id"""
    query = f"""
    SELECT 
        customer_unique_id,
        COUNT(*) as orders_per_customer,
        ROUND(SUM(total_order_value), 2) as customer_lifetime_value,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        DATE_DIFF(MAX(order_date), MIN(order_date), DAY) as customer_lifespan_days,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction_score
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    GROUP BY customer_unique_id
    ORDER BY orders_per_customer DESC, customer_lifetime_value DESC
    LIMIT 1000
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600) 
def get_customer_order_frequency():
    """Get distribution of orders per unique customer"""
    query = f"""
    WITH customer_order_counts AS (
        SELECT 
            customer_unique_id,
            COUNT(*) as orders_per_customer
        FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
        GROUP BY customer_unique_id
    )
    SELECT 
        orders_per_customer,
        COUNT(*) as unique_customers,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM customer_order_counts
    GROUP BY orders_per_customer
    ORDER BY orders_per_customer
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_customer_order_behavior():
    """Get customer order behavior distribution using new customer behavior fields"""
    query = f"""
    SELECT 
        customer_order_behavior,
        COUNT(DISTINCT customer_unique_id) as unique_customers,
        ROUND(COUNT(DISTINCT customer_unique_id) * 100.0 / 
              (SELECT COUNT(DISTINCT customer_unique_id) FROM {get_table_ref(ANALYTICS_TABLES["orders"])}), 2) as percentage,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        ROUND(AVG(customer_total_orders), 2) as avg_orders_in_category
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    GROUP BY customer_order_behavior
    ORDER BY 
        CASE customer_order_behavior
            WHEN 'single_order_customer' THEN 1
            WHEN 'two_order_customer' THEN 2
            WHEN 'regular_customer' THEN 3
            WHEN 'frequent_customer' THEN 4
            WHEN 'very_frequent_customer' THEN 5
        END
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_customer_lifetime_analysis():
    """Analyze customer lifetime metrics using new fields"""
    query = f"""
    SELECT 
        customer_unique_id,
        customer_total_orders,
        SUM(total_order_value) as customer_lifetime_value,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction,
        customer_order_behavior
    FROM {get_table_ref(ANALYTICS_TABLES["orders"])}
    GROUP BY customer_unique_id, customer_total_orders, customer_order_behavior
    ORDER BY customer_lifetime_value DESC
    LIMIT 1000
    """
    return execute_custom_query(query)

# =============================================================================
# MAIN DASHBOARD
# =============================================================================

try:
    # Load overview metrics
    with st.spinner("Loading order metrics..."):
        metrics_df = get_orders_overview_metrics()
    
    if metrics_df.empty:
        st.warning("No order data available. Please check your database connection.")
        st.stop()
    
    metrics = metrics_df.iloc[0]
    
    # Overview Metrics
    st.subheader("📊 Orders Overview")
    
    # Dataset explanation
    with st.expander("ℹ️ About Orders Analytics"):
        st.info("""
        **Order-Level Analysis Insights:**
        - Each order contains multiple items from potentially multiple sellers
        - Order complexity ranges from simple single-item orders to complex multi-seller orders
        - Analysis includes delivery performance, customer satisfaction, and logistics complexity
        - Financial metrics include total order value and payment behavior patterns
        """)
    
    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
    
    with col1:
        st.metric(
            label="Total Orders",
            value=f"{metrics['total_orders']:,}",
            help="Total number of orders placed"
        )
    
    with col2:
        st.metric(
            label="Unique Customers",
            value=f"{metrics['total_unique_customers']:,}",
            help="Number of unique customers (customer_unique_id)"
        )
    
    with col3:
        st.metric(
            label="Customer Records",
            value=f"{metrics['total_customer_records']:,}",
            help="Total customer records (customer_id per order)"
        )
    
    with col4:
        st.metric(
            label="Average Order Value",
            value=f"R$ {metrics['avg_order_value']:,.2f}",
            help="Average value per order including items and freight"
        )
    
    with col5:
        st.metric(
            label="Avg Items/Order",
            value=f"{metrics['avg_items_per_order']:.1f}",
            help="Average number of items per order"
        )
    
    with col6:
        st.metric(
            label="Avg Satisfaction",
            value=f"{metrics['avg_satisfaction_score']:.2f}/5",
            help="Average customer satisfaction score"
        )
    
    with col7:
        st.metric(
            label="Avg Orders/Customer",
            value=f"{metrics['avg_orders_per_customer']:.2f}",
            help="Average number of orders per unique customer"
        )
    
    st.markdown("---")
    
    # Customer Behavior Analysis Section
    st.subheader("👥 Customer Behavior Analysis")
    
    # Explanation about customer_unique_id vs customer_id
    with st.expander("ℹ️ Understanding Customer Metrics"):
        st.info("""
        **Customer Identification Fields:**
        - **Unique Customers (customer_unique_id)**: Represents the actual unique individuals across all orders
        - **Customer Records (customer_id)**: Individual customer records per order (may repeat for same person)
        - **Insight**: If Total Orders = Customer Records, it means each customer placed exactly one order
        """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Customer Order Frequency")
        with st.spinner("Loading customer frequency data..."):
            freq_df = get_customer_order_frequency()
        
        if not freq_df.empty:
            # Create bar chart for order frequency
            fig = px.bar(
                freq_df, 
                x='orders_per_customer', 
                y='unique_customers',
                title="Distribution of Orders per Unique Customer",
                labels={
                    'orders_per_customer': 'Orders per Customer',
                    'unique_customers': 'Number of Unique Customers'
                },
                color='unique_customers',
                color_continuous_scale=COLOR_PALETTES['primary']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Summary insights
            total_unique_customers = freq_df['unique_customers'].sum()
            single_order_customers = freq_df[freq_df['orders_per_customer'] == 1]['unique_customers'].sum()
            repeat_customers = total_unique_customers - single_order_customers
            
            st.info(f"""
            **Customer Behavior Insights:**
            - **{single_order_customers:,}** customers ({single_order_customers/total_unique_customers*100:.1f}%) placed only 1 order
            - **{repeat_customers:,}** customers ({repeat_customers/total_unique_customers*100:.1f}%) are repeat customers
            - **{freq_df['orders_per_customer'].max()}** maximum orders by a single customer
            """)
    
    with col2:
        st.subheader("💰 Customer Lifetime Value")
        with st.spinner("Loading customer behavior data..."):
            behavior_df = get_customer_behavior_analysis()
        
        if not behavior_df.empty:
            # Create scatter plot for CLV analysis
            fig = px.scatter(
                behavior_df.head(100),  # Top 100 customers
                x='orders_per_customer',
                y='customer_lifetime_value',
                size='avg_order_value',
                color='avg_satisfaction_score',
                title="Customer Lifetime Value vs Order Frequency (Top 100)",
                labels={
                    'orders_per_customer': 'Orders per Customer',
                    'customer_lifetime_value': 'Customer Lifetime Value (R$)',
                    'avg_satisfaction_score': 'Avg Satisfaction'
                },
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Top customers summary
            top_customers = behavior_df.head(10)
            st.info(f"""
            **Top Customer Insights:**
            - **Highest CLV**: R$ {behavior_df['customer_lifetime_value'].max():,.2f}
            - **Most Orders**: {behavior_df['orders_per_customer'].max()} orders
            - **Avg CLV**: R$ {behavior_df['customer_lifetime_value'].mean():,.2f}
            """)
    
    st.markdown("---")
    
    # Charts Section
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Monthly Orders Trend")
        with st.spinner("Loading trend data..."):
            trend_df = get_monthly_orders_trend()
        
        if not trend_df.empty:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Orders count
            fig.add_trace(
                go.Scatter(
                    x=trend_df['month_date'],
                    y=trend_df['order_count'],
                    name="Orders Count",
                    line=dict(color=COLOR_PALETTES['primary'][0], width=3)
                ),
                secondary_y=False,
            )
            
            # Average order value
            fig.add_trace(
                go.Scatter(
                    x=trend_df['month_date'],
                    y=trend_df['avg_order_value'],
                    name="Avg Order Value",
                    line=dict(color=COLOR_PALETTES['revenue'][0], width=2)
                ),
                secondary_y=True,
            )
            
            fig.update_xaxes(title_text="Month")
            fig.update_yaxes(title_text="Number of Orders", secondary_y=False)
            fig.update_yaxes(title_text="Average Order Value (R$)", secondary_y=True)
            fig.update_layout(height=400, title="Orders Trend Over Time")
            
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🎯 Order Complexity Distribution")
        with st.spinner("Loading complexity data..."):
            complexity_df = get_order_complexity_distribution()
        
        if not complexity_df.empty:
            fig = px.pie(
                complexity_df,
                values='order_count',
                names='order_complexity',
                title="Distribution by Complexity",
                color_discrete_sequence=COLOR_PALETTES['primary']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Order Value Analysis
    st.subheader("💰 Order Value Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Order Value Tiers")
        with st.spinner("Loading value tier data..."):
            value_tiers_df = get_order_value_tiers()
        
        if not value_tiers_df.empty:
            fig = px.bar(
                value_tiers_df,
                x='order_value_tier',
                y='order_count',
                title="Orders by Value Tier",
                color='total_revenue',
                color_continuous_scale='Viridis',
                text='order_count'
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Delivery Performance")
        with st.spinner("Loading delivery data..."):
            delivery_df = get_delivery_performance_analysis()
        
        if not delivery_df.empty:
            fig = px.bar(
                delivery_df,
                x='delivery_performance',
                y='order_count',
                title="Orders by Delivery Performance",
                color='avg_satisfaction',
                color_continuous_scale='RdYlGn',
                text='order_count'
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    # Geographic Analysis
    st.subheader("🗺️ Geographic Orders Analysis")
    
    with st.spinner("Loading geographic data..."):
        geo_df = get_geographic_orders_analysis()
    
    if not geo_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                geo_df.head(10),
                x='customer_state',
                y='order_count',
                title="Top 10 States by Order Count",
                color='avg_order_value',
                color_continuous_scale='Blues',
                text='order_count'
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Market tier analysis
            market_summary = geo_df.groupby('market_tier').agg({
                'order_count': 'sum',
                'total_revenue': 'sum',
                'avg_order_value': 'mean'
            }).reset_index()
            
            fig = px.sunburst(
                geo_df,
                path=['customer_region', 'market_tier', 'customer_state'],
                values='order_count',
                title="Orders by Region and Market Tier"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Advanced Analytics
    st.subheader("🔬 Advanced Order Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Satisfaction vs Complexity")
        with st.spinner("Loading satisfaction analysis..."):
            satisfaction_df = get_satisfaction_vs_complexity()
        
        if not satisfaction_df.empty:
            # Create heatmap
            pivot_df = satisfaction_df.pivot(
                index='order_complexity',
                columns='satisfaction_level',
                values='order_count'
            ).fillna(0)
            
            fig = px.imshow(
                pivot_df,
                title="Order Count: Complexity vs Satisfaction",
                color_continuous_scale='Viridis',
                text_auto=True
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Payment Behavior Analysis")
        with st.spinner("Loading payment behavior data..."):
            payment_df = get_payment_behavior_analysis()
        
        if not payment_df.empty:
            fig = px.scatter(
                payment_df,
                x='avg_installments',
                y='avg_order_value',
                size='order_count',
                color='avg_satisfaction',
                hover_data=['payment_behavior_type'],
                title="Payment Behavior: Installments vs Order Value",
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Customer Behavior Analysis Section
    st.markdown("---")
    st.subheader("🎯 Customer Behavior Analysis")
    
    # Customer order behavior distribution
    with st.spinner("Loading customer behavior data..."):
        behavior_data = get_customer_order_behavior()
    
    if not behavior_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Customer Order Behavior Distribution**")
            fig_behavior = px.pie(
                behavior_data,
                values='unique_customers',
                names='customer_order_behavior',
                title="Distribution of Customer Order Behaviors",
                color_discrete_map={
                    'single_order_customer': '#ff7f7f',
                    'two_order_customer': '#ffbf7f',
                    'regular_customer': '#7fbfff',
                    'frequent_customer': '#7fff7f',
                    'very_frequent_customer': '#bf7fff'
                }
            )
            fig_behavior.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_behavior, use_container_width=True)
        
        with col2:
            st.markdown("**Customer Behavior Metrics**")
            # Create a formatted table
            behavior_display = behavior_data.copy()
            behavior_display['customer_order_behavior'] = behavior_display['customer_order_behavior'].str.replace('_', ' ').str.title()
            behavior_display['percentage'] = behavior_display['percentage'].astype(str) + '%'
            behavior_display['avg_order_value'] = 'R$ ' + behavior_display['avg_order_value'].astype(str)
            behavior_display.columns = ['Behavior Type', 'Customers', 'Percentage', 'Avg Order Value', 'Avg Orders']
            st.dataframe(behavior_display, use_container_width=True, hide_index=True)
    
    # Customer Lifetime Value Analysis
    st.markdown("**💰 Top Customer Lifetime Value Analysis**")
    
    with st.spinner("Loading customer lifetime value data..."):
        lifetime_data = get_customer_lifetime_analysis()
    
    if not lifetime_data.empty:
        # Top customers by lifetime value
        col1, col2 = st.columns(2)
        
        with col1:
            # Top 20 customers chart
            top_customers = lifetime_data.head(20)
            fig_ltv = px.bar(
                top_customers,
                x='customer_lifetime_value',
                y=range(len(top_customers)),
                orientation='h',
                title="Top 20 Customers by Lifetime Value",
                labels={'customer_lifetime_value': 'Lifetime Value (R$)', 'y': 'Customer Rank'},
                color='customer_order_behavior',
                color_discrete_map={
                    'single_order_customer': '#ff7f7f',
                    'two_order_customer': '#ffbf7f',
                    'regular_customer': '#7fbfff',
                    'frequent_customer': '#7fff7f',
                    'very_frequent_customer': '#bf7fff'
                }
            )
            fig_ltv.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_ltv, use_container_width=True)
        
        with col2:
            # Customer lifetime value distribution by behavior
            ltv_by_behavior = lifetime_data.groupby('customer_order_behavior').agg({
                'customer_lifetime_value': ['mean', 'median', 'max'],
                'customer_total_orders': 'mean',
                'avg_satisfaction': 'mean'
            }).round(2)
            
            ltv_by_behavior.columns = ['Avg LTV', 'Median LTV', 'Max LTV', 'Avg Orders', 'Avg Satisfaction']
            ltv_by_behavior.index = ltv_by_behavior.index.str.replace('_', ' ').str.title()
            
            st.markdown("**Lifetime Value by Customer Behavior**")
            st.dataframe(ltv_by_behavior, use_container_width=True)
    
    # Detailed Data Tables
    st.subheader("📋 Detailed Analysis Tables")
    
    tab1, tab2, tab3, tab4 = st.tabs(["🎯 Complexity", "💰 Value Tiers", "🚚 Delivery", "💳 Payment"])
    
    with tab1:
        st.subheader("Order Complexity Analysis")
        if not complexity_df.empty:
            st.dataframe(
                complexity_df.style.format({
                    'order_count': '{:,}',
                    'avg_value': 'R$ {:,.2f}',
                    'avg_items': '{:.1f}',
                    'avg_logistics_score': '{:.1f}'
                }),
                use_container_width=True
            )
    
    with tab2:
        st.subheader("Order Value Tier Analysis")
        if not value_tiers_df.empty:
            st.dataframe(
                value_tiers_df.style.format({
                    'order_count': '{:,}',
                    'total_revenue': 'R$ {:,.2f}',
                    'avg_items': '{:.1f}',
                    'avg_satisfaction': '{:.2f}'
                }),
                use_container_width=True
            )
    
    with tab3:
        st.subheader("Delivery Performance Analysis")
        if not delivery_df.empty:
            st.dataframe(
                delivery_df.style.format({
                    'order_count': '{:,}',
                    'avg_fulfillment_days': '{:.1f}',
                    'avg_order_value': 'R$ {:,.2f}',
                    'avg_satisfaction': '{:.2f}'
                }),
                use_container_width=True
            )
    
    with tab4:
        st.subheader("Payment Behavior Analysis")
        if not payment_df.empty:
            st.dataframe(
                payment_df.style.format({
                    'order_count': '{:,}',
                    'avg_order_value': 'R$ {:,.2f}',
                    'avg_installments': '{:.1f}',
                    'avg_satisfaction': '{:.2f}'
                }),
                use_container_width=True
            )

except Exception as e:
    st.error(f"An error occurred while loading the dashboard: {str(e)}")
    st.info("Please check your database connection and ensure all required tables exist.")

# Footer
st.markdown("---")
st.markdown("""
**📦 Orders Analytics Dashboard**  
Comprehensive analysis of order patterns, complexity, delivery performance, and customer satisfaction.  
*Data refreshed hourly from the orders analytics OBT*
""")
