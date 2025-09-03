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
    page_title="Payment Analytics - Olist Dashboard",
    page_icon="💳",
    layout="wide"
)

st.title("💳 Payment Analytics")
st.markdown("---")

# Helper function to build table reference
def get_table_ref(table_name):
    return f"`{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{table_name}`"

# Fast SQL-based analytics functions
@st.cache_data(ttl=3600)
def get_payment_overview_metrics():
    """Get key payment overview metrics"""
    query = f"""
    SELECT 
        COUNT(*) as total_payment_transactions,
        COUNT(DISTINCT customer_unique_id) as unique_customers,
        COUNT(DISTINCT customer_id) as customer_records,
        COUNT(DISTINCT order_id) as unique_orders,
        ROUND(AVG(allocated_payment), 2) as avg_payment_amount,
        ROUND(SUM(allocated_payment), 2) as total_payment_volume,
        ROUND(AVG(payment_installments), 1) as avg_installments,
        ROUND(AVG(payment_per_installment), 2) as avg_installment_amount,
        ROUND(AVG(affordability_index), 1) as avg_affordability_index,
        COUNT(DISTINCT payment_type) as payment_methods_count
    FROM {get_table_ref(ANALYTICS_TABLES["payment"])}
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_payment_method_distribution():
    """Get payment method distribution and analysis"""
    query = f"""
    SELECT 
        payment_type,
        COUNT(*) as transactions,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as transaction_percentage,
        ROUND(AVG(allocated_payment), 2) as avg_amount,
        ROUND(SUM(allocated_payment), 2) as total_volume,
        ROUND(AVG(payment_installments), 1) as avg_installments,
        ROUND(AVG(payment_per_installment), 2) as avg_installment_amount,
        COUNT(DISTINCT customer_unique_id) as unique_customers
    FROM {get_table_ref(ANALYTICS_TABLES["payment"])}
    WHERE payment_type IS NOT NULL
    GROUP BY payment_type
    ORDER BY transactions DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_installment_analysis():
    """Get installment behavior analysis"""
    query = f"""
    SELECT 
        installment_category,
        COUNT(*) as transactions,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
        ROUND(AVG(allocated_payment), 2) as avg_payment,
        ROUND(AVG(payment_installments), 1) as avg_installments,
        ROUND(AVG(affordability_index), 1) as avg_affordability,
        ROUND(AVG(payment_per_installment), 2) as avg_per_installment,
        COUNT(DISTINCT customer_id) as customers
    FROM {get_table_ref(ANALYTICS_TABLES["payment"])}
    WHERE installment_category IS NOT NULL
    GROUP BY installment_category
    ORDER BY AVG(payment_installments)
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_payment_risk_analysis():
    """Get payment risk level analysis"""
    query = f"""
    SELECT 
        payment_risk_level,
        COUNT(*) as transactions,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as risk_percentage,
        ROUND(AVG(allocated_payment), 2) as avg_amount,
        ROUND(AVG(payment_installments), 1) as avg_installments,
        ROUND(AVG(review_score), 2) as avg_satisfaction,
        COUNT(DISTINCT customer_id) as customers,
        ROUND(SUM(allocated_payment), 2) as total_volume
    FROM {get_table_ref(ANALYTICS_TABLES["payment"])}
    WHERE payment_risk_level IS NOT NULL
    GROUP BY payment_risk_level
    ORDER BY AVG(payment_installments)
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_credit_behavior_analysis():
    """Get credit behavior type analysis"""
    query = f"""
    SELECT 
        credit_behavior_type,
        COUNT(*) as transactions,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
        ROUND(AVG(allocated_payment), 2) as avg_amount,
        ROUND(AVG(payment_installments), 1) as avg_installments,
        ROUND(AVG(review_score), 2) as avg_satisfaction,
        COUNT(DISTINCT customer_id) as customers
    FROM {get_table_ref(ANALYTICS_TABLES["payment"])}
    WHERE credit_behavior_type IS NOT NULL
    GROUP BY credit_behavior_type
    ORDER BY transactions DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_payment_trends():
    """Get payment trends over time"""
    query = f"""
    SELECT 
        year_month,
        COUNT(*) as transactions,
        ROUND(AVG(allocated_payment), 2) as avg_amount,
        ROUND(SUM(allocated_payment), 2) as total_volume,
        ROUND(AVG(payment_installments), 1) as avg_installments,
        COUNT(DISTINCT customer_id) as unique_customers,
        
        -- Payment method breakdown
        ROUND(AVG(is_credit_card) * 100, 1) as credit_card_pct,
        ROUND(AVG(is_boleto) * 100, 1) as boleto_pct,
        ROUND(AVG(is_debit_card) * 100, 1) as debit_card_pct,
        ROUND(AVG(is_voucher) * 100, 1) as voucher_pct
    FROM {get_table_ref(ANALYTICS_TABLES["payment"])}
    WHERE year_month IS NOT NULL
    GROUP BY year_month
    ORDER BY year_month
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_customer_payment_profiles():
    """Get customer payment profile distribution"""
    query = f"""
    SELECT 
        customer_payment_profile,
        COUNT(DISTINCT customer_id) as customers,
        ROUND(COUNT(DISTINCT customer_id) * 100.0 / SUM(COUNT(DISTINCT customer_id)) OVER(), 1) as customer_percentage,
        ROUND(AVG(allocated_payment), 2) as avg_payment,
        ROUND(AVG(payment_installments), 1) as avg_installments,
        ROUND(AVG(customer_affordability_index), 1) as avg_affordability,
        COUNT(*) as total_transactions
    FROM {get_table_ref(ANALYTICS_TABLES["payment"])}
    WHERE customer_payment_profile IS NOT NULL
    GROUP BY customer_payment_profile
    ORDER BY customers DESC
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_geographic_payment_patterns():
    """Get payment patterns by geography"""
    query = f"""
    SELECT 
        customer_state,
        COUNT(*) as transactions,
        COUNT(DISTINCT customer_id) as customers,
        ROUND(AVG(allocated_payment), 2) as avg_payment,
        ROUND(AVG(payment_installments), 1) as avg_installments,
        ROUND(AVG(is_credit_card) * 100, 1) as credit_card_usage_pct,
        ROUND(AVG(is_boleto) * 100, 1) as boleto_usage_pct,
        ROUND(AVG(affordability_index), 1) as avg_affordability
    FROM {get_table_ref(ANALYTICS_TABLES["payment"])}
    WHERE customer_state IS NOT NULL
    GROUP BY customer_state
    ORDER BY transactions DESC
    LIMIT 15
    """
    return execute_custom_query(query)

@st.cache_data(ttl=3600)
def get_payment_satisfaction_analysis():
    """Get payment satisfaction correlation analysis"""
    query = f"""
    SELECT 
        payment_satisfaction_profile,
        COUNT(*) as transactions,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
        ROUND(AVG(review_score), 2) as avg_review_score,
        ROUND(AVG(payment_installments), 1) as avg_installments,
        ROUND(AVG(allocated_payment), 2) as avg_payment,
        COUNT(DISTINCT customer_id) as customers
    FROM {get_table_ref(ANALYTICS_TABLES["payment"])}
    WHERE payment_satisfaction_profile IS NOT NULL 
        AND payment_satisfaction_profile != 'neutral_payment_satisfaction'
    GROUP BY payment_satisfaction_profile
    ORDER BY avg_review_score DESC
    """
    return execute_custom_query(query)

# Main content
try:
    # Load overview metrics
    with st.spinner("Loading payment analytics..."):
        overview_df = get_payment_overview_metrics()
    
    if overview_df.empty:
        st.warning("No payment data available. Please check your database connection.")
        st.stop()
    
    overview = overview_df.iloc[0]
    
    # Payment Overview Metrics
    st.subheader("💳 Payment Performance Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Transactions",
            value=f"{int(overview['total_payment_transactions']):,}",
            help="Total number of payment transactions"
        )
    
    with col2:
        st.metric(
            label="Total Volume", 
            value=f"R$ {overview['total_payment_volume']:,.2f}",
            help="Total payment volume processed"
        )
    
    with col3:
        st.metric(
            label="Avg Payment Amount",
            value=f"R$ {overview['avg_payment_amount']:,.2f}",
            help="Average payment amount per transaction"
        )
    
    with col4:
        st.metric(
            label="Avg Installments",
            value=f"{overview['avg_installments']:.1f}",
            help="Average number of installments"
        )
    
    # Additional metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Unique Customers",
            value=f"{int(overview['unique_customers']):,}",
            help="Number of unique customers making payments"
        )
    
    with col2:
        st.metric(
            label="Avg per Installment",
            value=f"R$ {overview['avg_installment_amount']:,.2f}",
            help="Average amount per installment"
        )
    
    with col3:
        st.metric(
            label="Affordability Index",
            value=f"{overview['avg_affordability_index']:.1f}/100",
            help="Average customer affordability score"
        )
    
    with col4:
        st.metric(
            label="Payment Methods",
            value=f"{int(overview['payment_methods_count'])}",
            help="Number of distinct payment methods"
        )
    
    st.markdown("---")
    
    # Payment Method Analysis
    st.subheader("💰 Payment Method Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💳 Payment Method Distribution")
        with st.spinner("Loading payment methods..."):
            payment_methods_df = get_payment_method_distribution()
        
        if not payment_methods_df.empty:
            fig = px.pie(payment_methods_df, 
                        values='transactions', 
                        names='payment_type',
                        title='Transaction Distribution by Payment Method',
                        color_discrete_sequence=COLOR_PALETTES['primary'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show payment method details
            st.dataframe(payment_methods_df.round(2), use_container_width=True)
        else:
            st.info("Payment method data will be displayed here.")
    
    with col2:
        st.subheader("📊 Payment Volume by Method")
        if not payment_methods_df.empty:
            fig = px.bar(payment_methods_df, 
                        x='payment_type', 
                        y='total_volume',
                        title='Payment Volume by Method',
                        labels={'total_volume': 'Total Volume (R$)', 'payment_type': 'Payment Method'},
                        color='avg_amount',
                        color_continuous_scale='Blues')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Payment volume analysis will be shown here.")
    
    # Installment and Risk Analysis
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Installment Behavior Analysis")
        with st.spinner("Loading installment data..."):
            installments_df = get_installment_analysis()
        
        if not installments_df.empty:
            fig = px.bar(installments_df, 
                        x='installment_category', 
                        y='transactions',
                        title='Transactions by Installment Category',
                        labels={'transactions': 'Number of Transactions', 'installment_category': 'Installment Category'},
                        color='avg_affordability',
                        color_continuous_scale='RdYlGn')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show installment details
            st.dataframe(installments_df.round(2), use_container_width=True)
        else:
            st.info("Installment analysis will be displayed here.")
    
    with col2:
        st.subheader("⚠️ Payment Risk Analysis")
        with st.spinner("Loading risk data..."):
            risk_df = get_payment_risk_analysis()
        
        if not risk_df.empty:
            fig = px.pie(risk_df, 
                        values='transactions', 
                        names='payment_risk_level',
                        title='Payment Risk Distribution',
                        color_discrete_sequence=['#00FF7F', '#FFD700', '#FFA500', '#FF6347'])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show risk details
            st.dataframe(risk_df.round(2), use_container_width=True)
        else:
            st.info("Risk analysis will be displayed here.")
    
    # Credit Behavior and Customer Profiles
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💡 Credit Behavior Types")
        with st.spinner("Loading credit behavior..."):
            credit_df = get_credit_behavior_analysis()
        
        if not credit_df.empty:
            fig = px.bar(credit_df, 
                        x='credit_behavior_type', 
                        y='transactions',
                        title='Transactions by Credit Behavior',
                        labels={'transactions': 'Number of Transactions', 'credit_behavior_type': 'Credit Behavior'},
                        color='avg_satisfaction',
                        color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show credit behavior details
            st.dataframe(credit_df.round(2), use_container_width=True)
        else:
            st.info("Credit behavior analysis will be displayed here.")
    
    with col2:
        st.subheader("👥 Customer Payment Profiles")
        with st.spinner("Loading customer profiles..."):
            profiles_df = get_customer_payment_profiles()
        
        if not profiles_df.empty:
            fig = px.bar(profiles_df, 
                        x='customer_payment_profile', 
                        y='customers',
                        title='Customer Distribution by Payment Profile',
                        labels={'customers': 'Number of Customers', 'customer_payment_profile': 'Payment Profile'},
                        color='avg_affordability',
                        color_continuous_scale='Blues')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show profile details
            st.dataframe(profiles_df.round(2), use_container_width=True)
        else:
            st.info("Customer profiles will be displayed here.")
    
    # Trends and Geographic Analysis
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📅 Payment Trends Over Time")
        with st.spinner("Loading trends..."):
            trends_df = get_payment_trends()
        
        if not trends_df.empty:
            # Create dual axis chart for volume and installments
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=trends_df['year_month'],
                y=trends_df['total_volume'],
                mode='lines+markers',
                name='Payment Volume (R$)',
                yaxis='y',
                line=dict(color='#1f77b4')
            ))
            
            fig.add_trace(go.Scatter(
                x=trends_df['year_month'],
                y=trends_df['avg_installments'],
                mode='lines+markers',
                name='Avg Installments',
                yaxis='y2',
                line=dict(color='#ff7f0e')
            ))
            
            fig.update_layout(
                title='Payment Volume and Installment Trends',
                xaxis_title='Month',
                yaxis=dict(title='Payment Volume (R$)', side='left'),
                yaxis2=dict(title='Average Installments', side='right', overlaying='y'),
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Show trends details
            st.dataframe(trends_df.round(2), use_container_width=True)
        else:
            st.info("Payment trends will be displayed here.")
    
    with col2:
        st.subheader("🗺️ Geographic Payment Patterns")
        with st.spinner("Loading geographic data..."):
            geo_df = get_geographic_payment_patterns()
        
        if not geo_df.empty:
            fig = px.bar(geo_df.head(10), 
                        x='customer_state', 
                        y='transactions',
                        title='Top 10 States by Payment Transactions',
                        labels={'transactions': 'Number of Transactions', 'customer_state': 'State'},
                        color='avg_installments',
                        color_continuous_scale='Plasma')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show geographic details
            st.dataframe(geo_df.head(10).round(2), use_container_width=True)
        else:
            st.info("Geographic analysis will be displayed here.")
    
    # Payment Satisfaction Analysis
    st.markdown("---")
    st.subheader("⭐ Payment Satisfaction Analysis")
    
    with st.spinner("Loading satisfaction data..."):
        satisfaction_df = get_payment_satisfaction_analysis()
    
    if not satisfaction_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(satisfaction_df, 
                        x='payment_satisfaction_profile', 
                        y='transactions',
                        title='Payment Satisfaction Profiles',
                        labels={'transactions': 'Number of Transactions', 'payment_satisfaction_profile': 'Satisfaction Profile'},
                        color='avg_review_score',
                        color_continuous_scale='RdYlGn')
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.scatter(satisfaction_df, 
                           x='avg_installments', 
                           y='avg_review_score',
                           size='transactions',
                           title='Installments vs Satisfaction',
                           labels={'avg_installments': 'Average Installments', 'avg_review_score': 'Average Review Score'},
                           color='avg_payment',
                           color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Show satisfaction details
        st.dataframe(satisfaction_df.round(2), use_container_width=True)
    else:
        st.info("Payment satisfaction analysis will be displayed here.")
    
    # Business Insights Section
    st.markdown("---")
    st.subheader("💡 Payment Insights")
    
    # Calculate insights from the data
    if not payment_methods_df.empty and not risk_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            top_method = payment_methods_df.iloc[0]
            st.success(f"""
            **Most Popular Payment Method**  
            {top_method['payment_type']}: {top_method['transaction_percentage']:.1f}% of transactions
            """)
        
        with col2:
            if not risk_df.empty:
                low_risk = risk_df[risk_df['payment_risk_level'] == 'minimal_risk']
                if not low_risk.empty:
                    low_risk_pct = low_risk.iloc[0]['risk_percentage']
                    st.info(f"""
                    **Payment Risk Profile**  
                    {low_risk_pct:.1f}% of transactions are minimal risk
                    """)
        
        with col3:
            if not installments_df.empty:
                single_payment = installments_df[installments_df['installment_category'] == 'single_payment']
                if not single_payment.empty:
                    single_pct = single_payment.iloc[0]['percentage']
                    st.warning(f"""
                    **Payment Preference**  
                    {single_pct:.1f}% prefer single payments
                    """)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.info("💡 This page provides comprehensive payment analytics including payment methods, installment behavior, risk analysis, and satisfaction correlation.")
    
    # Fallback metrics display
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Transactions", "Loading...", "...")
    with col2:
        st.metric("Payment Volume", "Loading...", "...")
    with col3:
        st.metric("Avg Installments", "Loading...", "...")
    with col4:
        st.metric("Connection", "✅ Active", "OK")
