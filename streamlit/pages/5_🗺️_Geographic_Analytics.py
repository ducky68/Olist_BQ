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
    page_title="Geographic Analytics - Olist Dashboard",
    page_icon="🗺️",
    layout="wide"
)

st.title("🗺️ Geographic Analytics")
st.markdown("---")

# Helper function to build table reference
def get_table_ref(table_name):
    return f"`{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{table_name}`"

# Fast SQL-based analytics functions
@st.cache_data(ttl=3600)
def get_geographic_overview_metrics():
    """Get key geographic overview metrics"""
    query = f"""
    SELECT 
        COUNT(DISTINCT state_code) as total_states,
        SUM(total_cities) as total_cities,
        SUM(total_customers) as total_customers,
        SUM(total_sellers) as total_sellers,
        SUM(total_orders) as total_orders,
        ROUND(SUM(total_revenue), 2) as total_revenue,
        ROUND(AVG(revenue_per_customer), 2) as avg_revenue_per_customer,
        ROUND(AVG(customers_per_seller), 2) as avg_customers_per_seller,
        ROUND(AVG(market_opportunity_index), 2) as avg_opportunity_index,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction_score
    FROM {get_table_ref(ANALYTICS_TABLES["geographic"])}
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_regional_performance():
    """Get performance by geographic regions"""
    query = f"""
    SELECT 
        geographic_region,
        COUNT(DISTINCT state_code) as states_count,
        SUM(total_customers) as customers,
        SUM(total_sellers) as sellers,
        SUM(total_orders) as orders,
        ROUND(SUM(total_revenue), 2) as revenue,
        ROUND(AVG(revenue_per_customer), 2) as avg_revenue_per_customer,
        ROUND(AVG(average_order_value), 2) as avg_order_value,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction,
        ROUND(AVG(market_opportunity_index), 2) as avg_opportunity_index
    FROM {get_table_ref(ANALYTICS_TABLES["geographic"])}
    WHERE geographic_region IS NOT NULL
    GROUP BY geographic_region
    ORDER BY revenue DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_market_tier_analysis():
    """Get market tier performance analysis"""
    query = f"""
    SELECT 
        market_tier,
        COUNT(DISTINCT state_code) as states,
        SUM(total_customers) as customers,
        SUM(total_sellers) as sellers,
        ROUND(SUM(total_revenue), 2) as revenue,
        ROUND(AVG(revenue_per_customer), 2) as avg_revenue_per_customer,
        ROUND(AVG(customers_per_seller), 2) as avg_customers_per_seller,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction,
        ROUND(AVG(market_opportunity_index), 2) as opportunity_index
    FROM {get_table_ref(ANALYTICS_TABLES["geographic"])}
    WHERE market_tier IS NOT NULL
    GROUP BY market_tier
    ORDER BY revenue DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_market_development_analysis():
    """Get market development tier analysis"""
    query = f"""
    SELECT 
        market_development_tier,
        COUNT(DISTINCT state_code) as states,
        SUM(total_customers) as customers,
        SUM(total_sellers) as sellers,
        ROUND(SUM(total_revenue), 2) as revenue,
        ROUND(AVG(revenue_per_customer), 2) as avg_revenue_per_customer,
        ROUND(AVG(days_active), 0) as avg_days_active,
        ROUND(AVG(market_opportunity_index), 2) as avg_opportunity_index
    FROM {get_table_ref(ANALYTICS_TABLES["geographic"])}
    WHERE market_development_tier IS NOT NULL
    GROUP BY market_development_tier
    ORDER BY revenue DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_state_performance_ranking():
    """Get top performing states"""
    query = f"""
    SELECT 
        state_code,
        geographic_region,
        market_tier,
        total_customers,
        total_sellers,
        total_orders,
        ROUND(total_revenue, 2) as revenue,
        ROUND(revenue_per_customer, 2) as revenue_per_customer,
        ROUND(average_order_value, 2) as avg_order_value,
        ROUND(avg_review_score, 2) as satisfaction,
        ROUND(market_opportunity_index, 2) as opportunity_index,
        total_cities,
        ROUND(customers_per_city, 1) as customers_per_city
    FROM {get_table_ref(ANALYTICS_TABLES["geographic"])}
    ORDER BY total_revenue DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_market_density_analysis():
    """Get market density analysis"""
    query = f"""
    SELECT 
        market_density,
        COUNT(DISTINCT state_code) as states,
        SUM(total_customers) as customers,
        SUM(total_cities) as cities,
        ROUND(AVG(customers_per_city), 1) as avg_customers_per_city,
        ROUND(AVG(revenue_per_city), 2) as avg_revenue_per_city,
        ROUND(SUM(total_revenue), 2) as total_revenue,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["geographic"])}
    WHERE market_density IS NOT NULL
    GROUP BY market_density
    ORDER BY avg_customers_per_city DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_payment_preferences_by_region():
    """Get payment preferences by geographic region"""
    query = f"""
    SELECT 
        geographic_region,
        payment_preference_profile,
        COUNT(DISTINCT state_code) as states,
        ROUND(AVG(credit_card_usage_pct), 1) as avg_credit_card_pct,
        ROUND(AVG(boleto_usage_pct), 1) as avg_boleto_pct,
        ROUND(AVG(avg_installments_used), 1) as avg_installments,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["geographic"])}
    WHERE geographic_region IS NOT NULL 
        AND payment_preference_profile IS NOT NULL
    GROUP BY geographic_region, payment_preference_profile
    ORDER BY geographic_region, avg_credit_card_pct DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_logistics_analysis():
    """Get logistics and shipping analysis by region"""
    query = f"""
    SELECT 
        geographic_region,
        logistics_profile,
        COUNT(DISTINCT state_code) as states,
        ROUND(AVG(local_shipping_pct), 1) as avg_local_shipping_pct,
        ROUND(AVG(cross_region_shipping_pct), 1) as avg_cross_region_pct,
        SUM(local_orders) as total_local_orders,
        SUM(cross_region_orders) as total_cross_region_orders,
        ROUND(AVG(avg_review_score), 2) as avg_satisfaction
    FROM {get_table_ref(ANALYTICS_TABLES["geographic"])}
    WHERE geographic_region IS NOT NULL 
        AND logistics_profile IS NOT NULL
    GROUP BY geographic_region, logistics_profile
    ORDER BY geographic_region, avg_local_shipping_pct DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_competition_analysis():
    """Get seller competition analysis"""
    query = f"""
    SELECT 
        seller_competition_level,
        COUNT(DISTINCT state_code) as states,
        SUM(total_customers) as customers,
        SUM(total_sellers) as sellers,
        ROUND(AVG(customers_per_seller), 1) as avg_customers_per_seller,
        ROUND(AVG(revenue_per_seller), 2) as avg_revenue_per_seller,
        ROUND(SUM(total_revenue), 2) as total_revenue,
        ROUND(AVG(market_opportunity_index), 2) as avg_opportunity_index
    FROM {get_table_ref(ANALYTICS_TABLES["geographic"])}
    WHERE seller_competition_level IS NOT NULL
    GROUP BY seller_competition_level
    ORDER BY avg_opportunity_index DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_market_maturity_analysis():
    """Get market maturity analysis"""
    query = f"""
    SELECT 
        market_maturity,
        COUNT(DISTINCT state_code) as states,
        SUM(total_customers) as customers,
        ROUND(AVG(days_active), 0) as avg_days_active,
        ROUND(AVG(months_active), 1) as avg_months_active,
        ROUND(AVG(market_activity_consistency), 2) as avg_consistency,
        ROUND(SUM(total_revenue), 2) as total_revenue,
        ROUND(AVG(market_opportunity_index), 2) as avg_opportunity_index
    FROM {get_table_ref(ANALYTICS_TABLES["geographic"])}
    WHERE market_maturity IS NOT NULL
    GROUP BY market_maturity
    ORDER BY avg_opportunity_index DESC
    """
    return execute_custom_query(query)

# Main content
try:
    # Load overview metrics
    with st.spinner("Loading geographic analytics..."):
        overview_df = get_geographic_overview_metrics()
    
    if overview_df.empty:
        st.warning("No geographic data available. Please check your database connection.")
        st.stop()
    
    overview = overview_df.iloc[0]
    
    # Geographic Overview Metrics
    st.subheader("🗺️ Geographic Market Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total States",
            value=f"{int(overview['total_states'])}",
            help="Number of Brazilian states with operations"
        )
    
    with col2:
        st.metric(
            label="Total Cities", 
            value=f"{int(overview['total_cities']):,}",
            help="Number of cities served"
        )
    
    with col3:
        st.metric(
            label="Total Revenue",
            value=f"R$ {overview['total_revenue']:,.2f}",
            help="Total revenue across all geographic markets"
        )
    
    with col4:
        st.metric(
            label="Market Opportunity",
            value=f"{overview['avg_opportunity_index']:.2f}",
            help="Average market opportunity index"
        )
    
    # Additional metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Customers",
            value=f"{int(overview['total_customers']):,}",
            help="Total customers across all regions"
        )
    
    with col2:
        st.metric(
            label="Avg Revenue/Customer",
            value=f"R$ {overview['avg_revenue_per_customer']:,.2f}",
            help="Average revenue per customer"
        )
    
    with col3:
        st.metric(
            label="Customers/Seller Ratio",
            value=f"{overview['avg_customers_per_seller']:.1f}",
            help="Average customers per seller"
        )
    
    with col4:
        st.metric(
            label="Avg Satisfaction",
            value=f"{overview['avg_satisfaction_score']:.2f} ⭐",
            help="Average customer satisfaction score"
        )
    
    st.markdown("---")
    
    # Regional Performance Analysis
    st.subheader("🌎 Regional Performance Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📍 Performance by Region")
        with st.spinner("Loading regional data..."):
            regional_df = get_regional_performance()
        
        if not regional_df.empty:
            fig = px.bar(regional_df, 
                        x='geographic_region', 
                        y='revenue',
                        title='Revenue by Geographic Region',
                        labels={'revenue': 'Revenue (R$)', 'geographic_region': 'Region'},
                        color='avg_opportunity_index',
                        color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show regional details
            st.dataframe(regional_df.round(2), use_container_width=True)
        else:
            st.info("Regional performance data will be displayed here.")
    
    with col2:
        st.subheader("🏆 Market Tier Analysis")
        with st.spinner("Loading market tiers..."):
            tiers_df = get_market_tier_analysis()
        
        if not tiers_df.empty:
            fig = px.pie(tiers_df, 
                        values='revenue', 
                        names='market_tier',
                        title='Revenue Distribution by Market Tier',
                        color_discrete_sequence=COLOR_PALETTES['geographic'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show tier details
            st.dataframe(tiers_df.round(2), use_container_width=True)
        else:
            st.info("Market tier analysis will be shown here.")
    
    # Market Development and State Rankings
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Market Development Stages")
        with st.spinner("Loading development data..."):
            development_df = get_market_development_analysis()
        
        if not development_df.empty:
            fig = px.bar(development_df, 
                        x='market_development_tier', 
                        y='states',
                        title='States by Development Stage',
                        labels={'states': 'Number of States', 'market_development_tier': 'Development Stage'},
                        color='avg_opportunity_index',
                        color_continuous_scale='RdYlGn')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show development details
            st.dataframe(development_df.round(2), use_container_width=True)
        else:
            st.info("Market development analysis will be displayed here.")
    
    with col2:
        st.subheader("🏅 Top Performing States")
        with st.spinner("Loading state rankings..."):
            states_df = get_state_performance_ranking()
        
        if not states_df.empty:
            # Show top 10 states by revenue
            top_states = states_df.head(10)
            fig = px.bar(top_states, 
                        x='state_code', 
                        y='revenue',
                        title='Top 10 States by Revenue',
                        labels={'revenue': 'Revenue (R$)', 'state_code': 'State'},
                        color='opportunity_index',
                        color_continuous_scale='Blues')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show top states table
            st.dataframe(top_states.round(2), use_container_width=True)
        else:
            st.info("State rankings will be displayed here.")
    
    # Market Density and Competition Analysis
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏙️ Market Density Analysis")
        with st.spinner("Loading density data..."):
            density_df = get_market_density_analysis()
        
        if not density_df.empty:
            fig = px.scatter(density_df, 
                           x='avg_customers_per_city', 
                           y='avg_revenue_per_city',
                           size='states',
                           title='Market Density: Customers vs Revenue per City',
                           labels={'avg_customers_per_city': 'Customers per City', 'avg_revenue_per_city': 'Revenue per City (R$)'},
                           color='market_density',
                           color_discrete_sequence=COLOR_PALETTES['primary'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show density details
            st.dataframe(density_df.round(2), use_container_width=True)
        else:
            st.info("Market density analysis will be displayed here.")
    
    with col2:
        st.subheader("⚔️ Competition Analysis")
        with st.spinner("Loading competition data..."):
            competition_df = get_competition_analysis()
        
        if not competition_df.empty:
            fig = px.bar(competition_df, 
                        x='seller_competition_level', 
                        y='states',
                        title='Competition Levels Across States',
                        labels={'states': 'Number of States', 'seller_competition_level': 'Competition Level'},
                        color='avg_opportunity_index',
                        color_continuous_scale='RdYlBu')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show competition details
            st.dataframe(competition_df.round(2), use_container_width=True)
        else:
            st.info("Competition analysis will be displayed here.")
    
    # Payment Preferences and Logistics
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💳 Regional Payment Preferences")
        with st.spinner("Loading payment preferences..."):
            payment_df = get_payment_preferences_by_region()
        
        if not payment_df.empty:
            # Create grouped bar chart for payment preferences
            fig = px.bar(payment_df, 
                        x='geographic_region', 
                        y='avg_credit_card_pct',
                        title='Credit Card Usage by Region',
                        labels={'avg_credit_card_pct': 'Credit Card Usage %', 'geographic_region': 'Region'},
                        color='payment_preference_profile',
                        color_discrete_sequence=COLOR_PALETTES['customer'])
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show payment details
            st.dataframe(payment_df.round(2), use_container_width=True)
        else:
            st.info("Payment preferences will be displayed here.")
    
    with col2:
        st.subheader("🚚 Logistics Patterns")
        with st.spinner("Loading logistics data..."):
            logistics_df = get_logistics_analysis()
        
        if not logistics_df.empty:
            fig = px.bar(logistics_df, 
                        x='geographic_region', 
                        y='avg_local_shipping_pct',
                        title='Local Shipping Percentage by Region',
                        labels={'avg_local_shipping_pct': 'Local Shipping %', 'geographic_region': 'Region'},
                        color='logistics_profile',
                        color_discrete_sequence=COLOR_PALETTES['revenue'])
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show logistics details
            st.dataframe(logistics_df.round(2), use_container_width=True)
        else:
            st.info("Logistics analysis will be displayed here.")
    
    # Market Maturity and Opportunity Analysis
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⏱️ Market Maturity Analysis")
        with st.spinner("Loading maturity data..."):
            maturity_df = get_market_maturity_analysis()
        
        if not maturity_df.empty:
            fig = px.pie(maturity_df, 
                        values='states', 
                        names='market_maturity',
                        title='Distribution of Market Maturity Levels',
                        color_discrete_sequence=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show maturity details
            st.dataframe(maturity_df.round(2), use_container_width=True)
        else:
            st.info("Market maturity analysis will be displayed here.")
    
    with col2:
        st.subheader("💎 Market Opportunity Insights")
        if not states_df.empty:
            # Opportunity vs Revenue scatter plot
            fig = px.scatter(states_df, 
                           x='opportunity_index', 
                           y='revenue',
                           size='total_customers',
                           title='Market Opportunity vs Revenue',
                           labels={'opportunity_index': 'Opportunity Index', 'revenue': 'Revenue (R$)'},
                           color='geographic_region',
                           hover_data=['state_code'],
                           color_discrete_sequence=COLOR_PALETTES['geographic'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Market opportunity analysis will be displayed here.")
    
    # Business Insights Section
    st.markdown("---")
    st.subheader("💡 Geographic Business Insights")
    
    # Calculate insights from the data
    if not regional_df.empty and not tiers_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if not regional_df.empty:
                top_region = regional_df.iloc[0]
                st.success(f"""
                **Leading Region**  
                {top_region['geographic_region']}: R$ {top_region['revenue']:,.2f} revenue
                """)
        
        with col2:
            if not tiers_df.empty:
                tier1 = tiers_df.iloc[0]
                st.info(f"""
                **Top Market Tier**  
                {tier1['market_tier']}: {tier1['opportunity_index']:.2f} opportunity index
                """)
        
        with col3:
            if not states_df.empty:
                top_state = states_df.iloc[0]
                st.warning(f"""
                **Best Performing State**  
                {top_state['state_code']}: {top_state['opportunity_index']:.2f} opportunity score
                """)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.info("💡 This page provides comprehensive geographic analytics including regional performance, market development, competition analysis, and opportunity insights.")
    
    # Fallback metrics display
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total States", "Loading...", "...")
    with col2:
        st.metric("Total Cities", "Loading...", "...")
    with col3:
        st.metric("Total Revenue", "Loading...", "...")
    with col4:
        st.metric("Connection", "✅ Active", "OK")
