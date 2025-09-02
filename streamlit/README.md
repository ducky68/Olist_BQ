# Streamlit Analytics Dashboard

This folder contains the Streamlit application for visualizing the Olist e-commerce analytics data.

## Structure

```
streamlit/
├── main.py                 # Main dashboard homepage
├── pages/                  # Individual analytics pages
│   ├── 1_📈_Revenue_Analytics.py
│   ├── 2_👥_Customer_Analytics.py  
│   ├── 3_🏪_Seller_Analytics.py
│   ├── 4_💳_Payment_Analytics.py
│   ├── 5_🗺️_Geographic_Analytics.py
│   └── 6_🚚_Delivery_Analytics.py
├── utils/                  # Shared utilities
│   ├── __init__.py
│   ├── database.py         # BigQuery connection utilities
│   ├── charts.py          # Reusable chart components
│   └── helpers.py         # General helper functions
├── config/                 # Configuration files
│   ├── __init__.py
│   └── settings.py        # App configuration
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Getting Started

1. **Install Dependencies:**
   ```bash
   cd streamlit
   pip install -r requirements.txt
   ```

2. **Run the Dashboard:**
   ```bash
   streamlit run main.py
   ```

3. **Access the Dashboard:**
   Open your browser to `http://localhost:8501`

## Pages Overview

- **Revenue Analytics**: Revenue trends, seasonal patterns, financial KPIs
- **Customer Analytics**: Customer segmentation, behavior analysis, lifetime value
- **Seller Analytics**: Seller performance, geographic distribution, business metrics  
- **Payment Analytics**: Payment methods, installment analysis, transaction patterns
- **Geographic Analytics**: State-wise performance, regional insights, geographic distribution
- **Delivery Analytics**: Shipping performance, delivery times, logistics efficiency

## Data Sources

All visualizations are powered by the comprehensive analytics OBT (One Big Table) models created in dbt:

- `revenue_analytics_obt` (112.6k rows)
- `customer_analytics_obt` 
- `seller_analytics_obt` (3.1k rows)
- `payment_analytics_obt` (112.6k rows)
- `geographic_analytics_obt` (27 rows)
- `delivery_analytics_obt` (112.6k rows)

## Development Notes

- Uses Streamlit's multi-page app structure
- BigQuery integration for real-time data access
- Responsive design with proper mobile support
- Interactive charts using Plotly and Altair
- Caching implemented for optimal performance
