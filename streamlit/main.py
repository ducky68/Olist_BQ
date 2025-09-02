"""
Olist E-commerce Analytics Dashboard
Main Streamlit application for Brazilian e-commerce data visualization
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# Configure page
st.set_page_config(
    page_title="Olist Analytics Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Main title
st.title("🛒 Olist E-commerce Analytics Dashboard")
st.markdown("---")

# Sidebar
st.sidebar.title("📊 Analytics Navigation")
st.sidebar.markdown("""
Welcome to the Olist E-commerce Analytics Dashboard!

Use the pages in the sidebar to explore different aspects of the business:
""")

# Main content
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="📈 Revenue Analytics",
        value="View Trends",
        help="Analyze revenue patterns, seasonal trends, and financial performance"
    )

with col2:
    st.metric(
        label="👥 Customer Analytics", 
        value="View Insights",
        help="Explore customer behavior, segmentation, and lifetime value"
    )

with col3:
    st.metric(
        label="🏪 Seller Analytics",
        value="View Performance", 
        help="Monitor seller performance, geographic distribution, and business metrics"
    )

st.markdown("---")

col4, col5, col6 = st.columns(3)

with col4:
    st.metric(
        label="💳 Payment Analytics",
        value="View Patterns",
        help="Understand payment methods, installment preferences, and transaction patterns"
    )

with col5:
    st.metric(
        label="🗺️ Geographic Analytics",
        value="View Distribution",
        help="Explore geographic patterns, state-wise performance, and regional insights"
    )

with col6:
    st.metric(
        label="🚚 Delivery Analytics", 
        value="View Logistics",
        help="Monitor delivery performance, shipping times, and logistics efficiency"
    )

# Instructions
st.markdown("---")
st.info("""
📍 **Getting Started:**
1. Use the sidebar navigation to explore different analytics sections
2. Each page provides interactive visualizations and insights
3. Data is sourced from our comprehensive dbt analytics models
4. All metrics are updated with the latest available data
""")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <p>Built with ❤️ using Streamlit and dbt • Brazilian E-commerce Data Analysis</p>
    </div>
    """, 
    unsafe_allow_html=True
)
