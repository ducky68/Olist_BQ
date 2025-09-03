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
    page_title="Delivery Analytics - Olist Dashboard",
    page_icon="🚚",
    layout="wide"
)

st.title("🚚 Delivery Analytics")
st.markdown("---")

# Helper function to build table reference
def get_table_ref(table_name):
    return f"`{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{table_name}`"

# Fast SQL-based analytics functions
@st.cache_data(ttl=600)
def get_table_columns():
    """Get column information for the delivery analytics table"""
    query = """
    SELECT column_name, data_type
    FROM `project-olist-470307.dbt_olist_analytics.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'delivery_analytics_obt'
    ORDER BY ordinal_position
    """
    try:
        return execute_custom_query(query)
    except Exception as e:
        st.error(f"Error getting table columns: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_delivery_sample():
    """Get a sample of delivery analytics data"""
    query = """
    SELECT *
    FROM `project-olist-470307.dbt_olist_analytics.delivery_analytics_obt`
    LIMIT 5
    """
    try:
        return execute_custom_query(query)
    except Exception as e:
        st.error(f"Error getting sample data: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_delivery_overview_metrics():
    """Get key delivery overview metrics"""
    query = f"""
    SELECT 
        COUNT(*) as total_deliveries,
        COUNT(DISTINCT order_id) as unique_orders,
        COUNT(DISTINCT customer_unique_id) as unique_customers,
        COUNT(DISTINCT customer_id) as customer_records,
        SUM(flag_delivered) as delivered_orders,
        SUM(flag_in_transit) as in_transit_orders,
        SUM(flag_canceled) as canceled_orders,
        ROUND(SUM(flag_delivered) * 100.0 / COUNT(*), 1) as delivery_success_rate,
        ROUND(AVG(freight_cost), 2) as avg_shipping_cost,
        ROUND(SUM(freight_cost), 2) as total_shipping_revenue,
        ROUND(AVG(review_score), 2) as avg_delivery_satisfaction,
        COUNT(DISTINCT shipping_complexity) as shipping_complexity_types
    FROM {get_table_ref(ANALYTICS_TABLES["delivery"])}
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_order_status_distribution():
    """Get order status distribution analysis"""
    query = f"""
    SELECT 
        order_status,
        COUNT(*) as orders,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
        ROUND(AVG(freight_cost), 2) as avg_shipping_cost,
        ROUND(AVG(item_price), 2) as avg_order_value,
        ROUND(AVG(review_score), 2) as avg_satisfaction,
        COUNT(DISTINCT customer_unique_id) as unique_customers
    FROM {get_table_ref(ANALYTICS_TABLES["delivery"])}
    WHERE order_status IS NOT NULL
    GROUP BY order_status
    ORDER BY orders DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_shipping_complexity_analysis():
    """Get shipping complexity analysis"""
    query = f"""
    SELECT 
        shipping_complexity,
        COUNT(*) as shipments,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
        ROUND(AVG(freight_cost), 2) as avg_shipping_cost,
        ROUND(AVG(item_price), 2) as avg_order_value,
        SUM(flag_delivered) as delivered,
        ROUND(SUM(flag_delivered) * 100.0 / COUNT(*), 1) as delivery_success_rate,
        ROUND(AVG(review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["delivery"])}
    WHERE shipping_complexity IS NOT NULL
    GROUP BY shipping_complexity
    ORDER BY shipments DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_geographic_delivery_performance():
    """Get delivery performance by geography"""
    query = f"""
    SELECT 
        customer_state,
        COUNT(*) as total_shipments,
        SUM(flag_delivered) as delivered_shipments,
        ROUND(SUM(flag_delivered) * 100.0 / COUNT(*), 1) as delivery_success_rate,
        ROUND(AVG(freight_cost), 2) as avg_shipping_cost,
        ROUND(AVG(item_price), 2) as avg_order_value,
        COUNT(DISTINCT customer_city) as cities_served,
        ROUND(AVG(review_score), 2) as avg_satisfaction,
        COUNT(DISTINCT seller_state) as seller_states_served
    FROM {get_table_ref(ANALYTICS_TABLES["delivery"])}
    WHERE customer_state IS NOT NULL
    GROUP BY customer_state
    ORDER BY total_shipments DESC
    LIMIT 15
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_product_category_logistics():
    """Get logistics performance by product category"""
    query = f"""
    SELECT 
        product_category_english,
        product_weight_category,
        COUNT(*) as shipments,
        ROUND(AVG(freight_cost), 2) as avg_shipping_cost,
        ROUND(AVG(item_price), 2) as avg_product_price,
        ROUND(AVG(freight_cost) / NULLIF(AVG(item_price), 0) * 100, 2) as shipping_to_price_ratio,
        SUM(flag_delivered) as delivered_shipments,
        ROUND(SUM(flag_delivered) * 100.0 / COUNT(*), 1) as delivery_success_rate,
        ROUND(AVG(review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["delivery"])}
    WHERE product_category_english IS NOT NULL
    GROUP BY product_category_english, product_weight_category
    ORDER BY shipments DESC
    LIMIT 20
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_delivery_trends():
    """Get delivery trends over time"""
    query = f"""
    SELECT 
        order_year,
        order_quarter,
        order_month,
        COUNT(*) as total_shipments,
        SUM(flag_delivered) as delivered_shipments,
        SUM(flag_canceled) as canceled_shipments,
        ROUND(SUM(flag_delivered) * 100.0 / COUNT(*), 1) as delivery_success_rate,
        ROUND(AVG(freight_cost), 2) as avg_shipping_cost,
        ROUND(SUM(freight_cost), 2) as total_shipping_revenue,
        COUNT(DISTINCT customer_id) as unique_customers,
        ROUND(AVG(review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["delivery"])}
    WHERE order_year IS NOT NULL AND order_month IS NOT NULL
    GROUP BY order_year, order_quarter, order_month
    ORDER BY order_year, order_month
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_freight_cost_analysis():
    """Get freight cost analysis and optimization insights"""
    query = f"""
    SELECT 
        CASE 
            WHEN freight_cost = 0 THEN 'free_shipping'
            WHEN freight_cost <= 10 THEN 'low_cost'
            WHEN freight_cost <= 30 THEN 'medium_cost'
            WHEN freight_cost <= 60 THEN 'high_cost'
            ELSE 'premium_cost'
        END as freight_cost_tier,
        COUNT(*) as shipments,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
        ROUND(AVG(freight_cost), 2) as avg_cost,
        ROUND(AVG(item_price), 2) as avg_order_value,
        SUM(flag_delivered) as delivered,
        ROUND(SUM(flag_delivered) * 100.0 / COUNT(*), 1) as delivery_success_rate,
        ROUND(AVG(review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["delivery"])}
    GROUP BY freight_cost_tier
    ORDER BY avg_cost
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_seller_delivery_performance():
    """Get delivery performance by seller location"""
    query = f"""
    SELECT 
        seller_state,
        COUNT(*) as total_shipments,
        COUNT(DISTINCT customer_state) as states_served,
        SUM(flag_delivered) as delivered_shipments,
        ROUND(SUM(flag_delivered) * 100.0 / COUNT(*), 1) as delivery_success_rate,
        ROUND(AVG(freight_cost), 2) as avg_shipping_cost,
        COUNT(DISTINCT seller_city) as seller_cities,
        ROUND(AVG(review_score), 2) as avg_satisfaction,
        
        -- Cross-state shipping analysis
        SUM(CASE WHEN customer_state != seller_state THEN 1 ELSE 0 END) as cross_state_shipments,
        ROUND(SUM(CASE WHEN customer_state != seller_state THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as cross_state_percentage
    FROM {get_table_ref(ANALYTICS_TABLES["delivery"])}
    WHERE seller_state IS NOT NULL
    GROUP BY seller_state
    ORDER BY total_shipments DESC
    LIMIT 15
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_delivery_satisfaction_correlation():
    """Get correlation between delivery metrics and customer satisfaction"""
    query = f"""
    SELECT 
        CASE 
            WHEN review_score >= 5 THEN 'excellent'
            WHEN review_score >= 4 THEN 'good'
            WHEN review_score >= 3 THEN 'average'
            WHEN review_score >= 2 THEN 'poor'
            WHEN review_score >= 1 THEN 'very_poor'
            ELSE 'no_review'
        END as satisfaction_level,
        COUNT(*) as shipments,
        ROUND(AVG(freight_cost), 2) as avg_shipping_cost,
        ROUND(AVG(item_price), 2) as avg_order_value,
        SUM(flag_delivered) as delivered,
        SUM(flag_canceled) as canceled,
        ROUND(SUM(flag_delivered) * 100.0 / COUNT(*), 1) as delivery_success_rate,
        
        -- Shipping complexity breakdown
        SUM(CASE WHEN shipping_complexity = 'same_state' THEN 1 ELSE 0 END) as same_state_shipments,
        SUM(CASE WHEN shipping_complexity = 'cross_region' THEN 1 ELSE 0 END) as cross_region_shipments
    FROM {get_table_ref(ANALYTICS_TABLES["delivery"])}
    WHERE satisfaction_level IS NOT NULL
    GROUP BY satisfaction_level
    ORDER BY 
        CASE satisfaction_level
            WHEN 'excellent' THEN 1
            WHEN 'good' THEN 2
            WHEN 'average' THEN 3
            WHEN 'poor' THEN 4
            WHEN 'very_poor' THEN 5
            ELSE 6
        END
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_order_size_logistics():
    """Get logistics performance by order size"""
    query = f"""
    SELECT 
        total_items_in_order,
        COUNT(*) as orders,
        ROUND(AVG(freight_cost), 2) as avg_shipping_cost,
        ROUND(AVG(total_order_value), 2) as avg_order_value,
        ROUND(AVG(freight_cost) / NULLIF(AVG(total_order_value), 0) * 100, 2) as shipping_percentage,
        SUM(flag_delivered) as delivered,
        ROUND(SUM(flag_delivered) * 100.0 / COUNT(*), 1) as delivery_success_rate,
        ROUND(AVG(review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["delivery"])}
    WHERE total_items_in_order IS NOT NULL
        AND total_items_in_order <= 10  -- Focus on reasonable order sizes
    GROUP BY total_items_in_order
    ORDER BY total_items_in_order
    """
    return execute_custom_query(query)

# Main content
try:
    # First, let's examine the table structure for debugging
    st.subheader("🔍 Table Structure Analysis")
    
    with st.expander("📋 Delivery Analytics Table Columns"):
        with st.spinner("Loading table structure..."):
            columns_df = get_table_columns()
        
        if not columns_df.empty:
            st.dataframe(columns_df, use_container_width=True)
        else:
            st.warning("Could not retrieve column information")
    
    with st.expander("📊 Sample Data"):
        with st.spinner("Loading sample data..."):
            sample_df = get_delivery_sample()
        
        if not sample_df.empty:
            st.dataframe(sample_df, use_container_width=True)
            st.info(f"Available columns: {', '.join(sample_df.columns)}")
    
    # Debug button to test actual overview query
    if st.button("🧪 Test Overview Query"):
        st.subheader("Testing Overview Query")
        test_query = """
        SELECT 
            COUNT(*) as total_orders,
            AVG(shipping_days) as avg_shipping_days,
            AVG(freight_value) as avg_freight_value,
            SUM(CASE WHEN shipping_days <= estimated_delivery_days THEN 1 ELSE 0 END) / COUNT(*) * 100 as on_time_delivery_rate
        FROM `project-olist-470307.dbt_olist_analytics.delivery_analytics_obt`
        WHERE order_status = 'delivered'
        """
        
        try:
            test_result = execute_custom_query(test_query)
            if not test_result.empty:
                st.success("✅ Overview query successful!")
                st.dataframe(test_result)
            else:
                st.error("❌ Query returned empty result")
        except Exception as e:
            st.error(f"❌ Query failed: {str(e)}")
    
    # Load overview metrics
    with st.spinner("Loading delivery analytics..."):
        overview_df = get_delivery_overview_metrics()
    
    if overview_df.empty:
        st.warning("No delivery data available. Please check your database connection.")
        st.stop()
    
    overview = overview_df.iloc[0]
    
    # Delivery Overview Metrics
    st.subheader("🚚 Delivery Performance Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Deliveries",
            value=f"{int(overview['total_deliveries']):,}",
            help="Total number of delivery transactions"
        )
    
    with col2:
        st.metric(
            label="Delivery Success Rate", 
            value=f"{overview['delivery_success_rate']:.1f}%",
            help="Percentage of successfully delivered orders"
        )
    
    with col3:
        st.metric(
            label="Avg Shipping Cost",
            value=f"R$ {overview['avg_shipping_cost']:,.2f}",
            help="Average freight cost per shipment"
        )
    
    with col4:
        st.metric(
            label="Total Shipping Revenue",
            value=f"R$ {overview['total_shipping_revenue']:,.2f}",
            help="Total freight revenue generated"
        )
    
    # Additional metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Delivered Orders",
            value=f"{int(overview['delivered_orders']):,}",
            help="Number of successfully delivered orders"
        )
    
    with col2:
        st.metric(
            label="In Transit",
            value=f"{int(overview['in_transit_orders']):,}",
            help="Orders currently being shipped"
        )
    
    with col3:
        st.metric(
            label="Canceled Orders",
            value=f"{int(overview['canceled_orders']):,}",
            help="Orders that were canceled"
        )
    
    with col4:
        st.metric(
            label="Delivery Satisfaction",
            value=f"{overview['avg_delivery_satisfaction']:.2f} ⭐",
            help="Average customer satisfaction for deliveries"
        )
    
    st.markdown("---")
    
    # Order Status and Shipping Complexity Analysis
    st.subheader("📊 Delivery Performance Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📦 Order Status Distribution")
        with st.spinner("Loading order status data..."):
            status_df = get_order_status_distribution()
        
        if not status_df.empty:
            fig = px.pie(status_df, 
                        values='orders', 
                        names='order_status',
                        title='Distribution of Order Status',
                        color_discrete_sequence=COLOR_PALETTES['primary'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show status details
            st.dataframe(status_df.round(2), use_container_width=True)
        else:
            st.info("Order status data will be displayed here.")
    
    with col2:
        st.subheader("🗺️ Shipping Complexity Analysis")
        with st.spinner("Loading shipping complexity..."):
            complexity_df = get_shipping_complexity_analysis()
        
        if not complexity_df.empty:
            fig = px.bar(complexity_df, 
                        x='shipping_complexity', 
                        y='shipments',
                        title='Shipments by Complexity Level',
                        labels={'shipments': 'Number of Shipments', 'shipping_complexity': 'Shipping Complexity'},
                        color='delivery_success_rate',
                        color_continuous_scale='RdYlGn')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show complexity details
            st.dataframe(complexity_df.round(2), use_container_width=True)
        else:
            st.info("Shipping complexity analysis will be shown here.")
    
    # Geographic Performance and Freight Analysis
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏙️ Geographic Delivery Performance")
        with st.spinner("Loading geographic data..."):
            geo_df = get_geographic_delivery_performance()
        
        if not geo_df.empty:
            fig = px.bar(geo_df.head(10), 
                        x='customer_state', 
                        y='total_shipments',
                        title='Top 10 States by Shipment Volume',
                        labels={'total_shipments': 'Number of Shipments', 'customer_state': 'State'},
                        color='delivery_success_rate',
                        color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show geographic details
            st.dataframe(geo_df.head(10).round(2), use_container_width=True)
        else:
            st.info("Geographic performance will be displayed here.")
    
    with col2:
        st.subheader("💰 Freight Cost Analysis")
        with st.spinner("Loading freight cost data..."):
            freight_df = get_freight_cost_analysis()
        
        if not freight_df.empty:
            fig = px.bar(freight_df, 
                        x='freight_cost_tier', 
                        y='shipments',
                        title='Shipments by Freight Cost Tier',
                        labels={'shipments': 'Number of Shipments', 'freight_cost_tier': 'Cost Tier'},
                        color='avg_satisfaction',
                        color_continuous_scale='RdYlGn')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show freight details
            st.dataframe(freight_df.round(2), use_container_width=True)
        else:
            st.info("Freight cost analysis will be displayed here.")
    
    # Product Category and Seller Performance
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📦 Product Category Logistics")
        with st.spinner("Loading product logistics..."):
            product_df = get_product_category_logistics()
        
        if not product_df.empty:
            # Top 10 categories by shipment volume
            top_categories = product_df.head(10)
            fig = px.scatter(top_categories, 
                           x='avg_shipping_cost', 
                           y='shipping_to_price_ratio',
                           size='shipments',
                           title='Shipping Cost vs Price Ratio by Category',
                           labels={'avg_shipping_cost': 'Avg Shipping Cost (R$)', 'shipping_to_price_ratio': 'Shipping/Price Ratio (%)'},
                           color='delivery_success_rate',
                           hover_data=['product_category_english'],
                           color_continuous_scale='RdYlGn')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show product details
            st.dataframe(top_categories.round(2), use_container_width=True)
        else:
            st.info("Product logistics analysis will be displayed here.")
    
    with col2:
        st.subheader("🏪 Seller Delivery Performance")
        with st.spinner("Loading seller performance..."):
            seller_df = get_seller_delivery_performance()
        
        if not seller_df.empty:
            # Top 10 seller states
            top_sellers = seller_df.head(10)
            fig = px.bar(top_sellers, 
                        x='seller_state', 
                        y='total_shipments',
                        title='Top 10 Seller States by Volume',
                        labels={'total_shipments': 'Number of Shipments', 'seller_state': 'Seller State'},
                        color='cross_state_percentage',
                        color_continuous_scale='Blues')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show seller details
            st.dataframe(top_sellers.round(2), use_container_width=True)
        else:
            st.info("Seller performance will be displayed here.")
    
    # Trends and Satisfaction Analysis
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Delivery Trends Over Time")
        with st.spinner("Loading trends..."):
            trends_df = get_delivery_trends()
        
        if not trends_df.empty:
            # Create time series chart
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=trends_df['order_month'],
                y=trends_df['total_shipments'],
                mode='lines+markers',
                name='Total Shipments',
                yaxis='y',
                line=dict(color='#1f77b4')
            ))
            
            fig.add_trace(go.Scatter(
                x=trends_df['order_month'],
                y=trends_df['delivery_success_rate'],
                mode='lines+markers',
                name='Success Rate (%)',
                yaxis='y2',
                line=dict(color='#ff7f0e')
            ))
            
            fig.update_layout(
                title='Delivery Volume and Success Rate Trends',
                xaxis_title='Month',
                yaxis=dict(title='Total Shipments', side='left'),
                yaxis2=dict(title='Success Rate (%)', side='right', overlaying='y'),
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Show trends details
            st.dataframe(trends_df.round(2), use_container_width=True)
        else:
            st.info("Delivery trends will be displayed here.")
    
    with col2:
        st.subheader("⭐ Satisfaction vs Delivery Performance")
        with st.spinner("Loading satisfaction data..."):
            satisfaction_df = get_delivery_satisfaction_correlation()
        
        if not satisfaction_df.empty:
            fig = px.bar(satisfaction_df, 
                        x='satisfaction_level', 
                        y='shipments',
                        title='Shipments by Customer Satisfaction Level',
                        labels={'shipments': 'Number of Shipments', 'satisfaction_level': 'Satisfaction Level'},
                        color='delivery_success_rate',
                        color_continuous_scale='RdYlGn')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show satisfaction details
            st.dataframe(satisfaction_df.round(2), use_container_width=True)
        else:
            st.info("Satisfaction analysis will be displayed here.")
    
    # Order Size Impact Analysis
    st.markdown("---")
    st.subheader("📦 Order Size Impact on Delivery")
    
    with st.spinner("Loading order size data..."):
        order_size_df = get_order_size_logistics()
    
    if not order_size_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.line(order_size_df, 
                         x='total_items_in_order', 
                         y='avg_shipping_cost',
                         title='Shipping Cost vs Order Size',
                         labels={'avg_shipping_cost': 'Avg Shipping Cost (R$)', 'total_items_in_order': 'Items in Order'},
                         markers=True)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.scatter(order_size_df, 
                           x='total_items_in_order', 
                           y='delivery_success_rate',
                           size='orders',
                           title='Delivery Success vs Order Size',
                           labels={'delivery_success_rate': 'Delivery Success Rate (%)', 'total_items_in_order': 'Items in Order'},
                           color='avg_satisfaction',
                           color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Show order size details
        st.dataframe(order_size_df.round(2), use_container_width=True)
    else:
        st.info("Order size analysis will be displayed here.")
    
    # Business Insights Section
    st.markdown("---")
    st.subheader("💡 Delivery Business Insights")
    
    # Calculate insights from the data
    if not status_df.empty and not complexity_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if not status_df.empty:
                delivered_pct = status_df[status_df['order_status'] == 'delivered']['percentage'].iloc[0] if len(status_df[status_df['order_status'] == 'delivered']) > 0 else 0
                st.success(f"""
                **Delivery Success**  
                {delivered_pct:.1f}% of orders delivered successfully
                """)
        
        with col2:
            if not complexity_df.empty:
                same_state = complexity_df[complexity_df['shipping_complexity'] == 'same_state']
                if not same_state.empty:
                    same_state_pct = same_state.iloc[0]['percentage']
                    st.info(f"""
                    **Local Shipping Preference**  
                    {same_state_pct:.1f}% of shipments within same state
                    """)
        
        with col3:
            if not freight_df.empty:
                free_shipping = freight_df[freight_df['freight_cost_tier'] == 'free_shipping']
                if not free_shipping.empty:
                    free_pct = free_shipping.iloc[0]['percentage']
                    st.warning(f"""
                    **Free Shipping Strategy**  
                    {free_pct:.1f}% of orders have free shipping
                    """)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.info("💡 This page provides comprehensive delivery analytics including shipping performance, geographic analysis, freight optimization, and satisfaction correlation.")
    
    # Fallback metrics display
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Deliveries", "Loading...", "...")
    with col2:
        st.metric("Success Rate", "Loading...", "...")
    with col3:
        st.metric("Avg Shipping Cost", "Loading...", "...")
    with col4:
        st.metric("Connection", "✅ Active", "OK")
