"""
Configuration settings for the Streamlit dashboard
"""

# BigQuery Configuration
BIGQUERY_CONFIG = {
    "project_id": "project-olist-470307",
    "dataset_id": "dbt_olist_analytics",  # Corrected to the working dataset
    "location": "asia-southeast1"  # Updated to your data location
}

# Analytics OBT Table Names
ANALYTICS_TABLES = {
    "revenue": "revenue_analytics_obt",
    "customer": "customer_analytics_obt", 
    "seller": "seller_analytics_obt",
    "payment": "payment_analytics_obt",
    "geographic": "geographic_analytics_obt",
    "delivery": "delivery_analytics_obt",
    "orders": "orders_analytics_obt"
}

# Streamlit Page Configuration
PAGE_CONFIG = {
    "page_title": "Olist Analytics Dashboard",
    "page_icon": "🛒",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Chart Color Palettes
COLOR_PALETTES = {
    "primary": ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"],
    "revenue": ["#2E8B57", "#32CD32", "#90EE90", "#98FB98", "#00FF7F"],
    "customer": ["#4169E1", "#6495ED", "#87CEEB", "#87CEFA", "#B0E0E6"],
    "geographic": ["#FF6347", "#FFA07A", "#FFB6C1", "#FFC0CB", "#FFE4E1"]
}

# Default Chart Settings
CHART_DEFAULTS = {
    "height": 400,
    "theme": "streamlit",
    "use_container_width": True
}

# Cache Settings (in seconds)
CACHE_TTL = {
    "data_queries": 3600,  # 1 hour
    "table_info": 7200,    # 2 hours  
    "charts": 1800         # 30 minutes
}
