# Olist E-commerce Analytics Project

## 📋 Table of Contents
1. [Project Overview](#project-overview)
2. [Environment Setup](#environment-setup)
3. [GCP BigQuery Configuration](#gcp-bigquery-configuration)
4. [dbt Data Architecture](#dbt-data-architecture)
5. [Analytics OBT Layer - Detailed Structure](#analytics-obt-layer---detailed-structure)
6. [Streamlit Dashboard](#streamlit-dashboard)
7. [Quick Start Guide](#quick-start-guide)
8. [Development & Testing](#development--testing)

---

## 🎯 Project Overview

Complete end-to-end analytics and visualization platform for Brazilian e-commerce data (Olist) using modern data stack: **dbt + BigQuery + Streamlit**. This project demonstrates advanced data modeling patterns including staging, dimensional warehouse, and analytics-ready OBT (One Big Table) layers.

### Key Features
- **Multi-layered Data Architecture**: Staging → Warehouse → Analytics
- **6 Comprehensive Analytics Modules**: Revenue, Customer, Seller, Payment, Geographic, Delivery
- **Interactive Streamlit Dashboard**: Real-time insights and visualizations
- **Production-Ready**: Full data quality testing, documentation, and CI/CD patterns

### Project Structure

```
Olist_BQ/
├── dbt/                           # dbt Analytics Layer
│   ├── models/
│   │   ├── staging/               # Raw data staging & cleaning
│   │   │   ├── source.yml         # Source definitions
│   │   │   ├── schema.yml         # Staging model schemas
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_order_payments.sql
│   │   │   ├── stg_order_reviews.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_sellers.sql
│   │   │   ├── stg_geolocation.sql
│   │   │   └── stg_product_category_name_translation.sql
│   │   ├── warehouse/             # Star schema dimensional model
│   │   │   ├── schema.yml         # Warehouse model schemas
│   │   │   ├── fact_order_items.sql      # Central fact table
│   │   │   ├── dim_customer.sql          # Customer dimension
│   │   │   ├── dim_product.sql           # Product dimension
│   │   │   ├── dim_seller.sql            # Seller dimension
│   │   │   ├── dim_orders.sql            # Order dimension
│   │   │   ├── dim_payment.sql           # Payment dimension
│   │   │   ├── dim_order_reviews.sql     # Review dimension
│   │   │   ├── dim_date.sql              # Date dimension
│   │   │   ├── dim_geolocation.sql       # Geographic dimension
│   │   │   ├── Star_Schema_Design.md     # Architecture documentation
│   │   │   └── Star_Schema_DDL.txt       # SQL DDL statements
│   │   └── analytics_obt/         # One Big Tables for BI/Analytics
│   │       ├── schema.yml         # Analytics schemas with tests
│   │       ├── README.md          # Analytics layer documentation
│   │       ├── revenue_analytics_obt.sql    # Revenue insights
│   │       ├── customer_analytics_obt.sql   # Customer behavior
│   │       ├── seller_analytics_obt.sql     # Seller performance
│   │       ├── payment_analytics_obt.sql    # Payment patterns
│   │       ├── geographic_analytics_obt.sql # Regional analysis
│   │       └── delivery_analytics_obt.sql   # Logistics insights
│   ├── macros/                    # Reusable Jinja macros
│   ├── analyses/                  # Ad-hoc SQL queries
│   ├── tests/                     # Data quality tests
│   ├── logs/                      # dbt execution logs
│   └── dbt_project.yml           # dbt configuration
├── streamlit/                     # Visualization Dashboard
│   ├── pages/                     # Individual analytics pages
│   │   ├── 1_📈_Revenue_Analytics.py
│   │   ├── 2_👥_Customer_Analytics.py
│   │   ├── 3_🏪_Seller_Analytics.py
│   │   ├── 4_💳_Payment_Analytics.py
│   │   ├── 5_🗺️_Geographic_Analytics.py
│   │   ├── 6_🚚_Delivery_Analytics.py
│   │   └── 🔧_Connection_Test.py
│   ├── utils/                     # Database & helper utilities
│   ├── config/                    # App configuration
│   ├── main.py                   # Main dashboard
│   ├── requirements.txt          # Streamlit dependencies
│   └── README.md                 # Dashboard documentation
├── environment.yml               # Conda environment setup
├── requirements.txt              # Complete project dependencies
├── .git/                         # Git repository
└── README.md                     # This comprehensive documentation
```

---

## 🔧 Environment Setup

### Option 1: Conda Environment (Recommended)

```bash
# Create environment from file
conda env create -f environment.yml

# Activate environment
conda activate olist_analytics

# Verify installation
dbt --version
streamlit --version
```

### Option 2: Pip Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install all dependencies
pip install -r requirements.txt
```

### Key Dependencies
- **dbt-core**: 1.6.0+ (Data transformation framework)
- **dbt-bigquery**: 1.6.0+ (BigQuery adapter)
- **streamlit**: 1.28.0+ (Dashboard framework)
- **google-cloud-bigquery**: 3.11.0+ (GCP connectivity)
- **plotly**: 5.15.0+ (Interactive visualizations)
- **pandas**: 2.0.0+ (Data manipulation)

---

## 🏗️ GCP BigQuery Configuration

### Google Cloud Project Setup

**Project ID**: `project-olist-470307`

### profiles.yml Configuration

Create `~/.dbt/profiles.yml` for dbt BigQuery connection:

```yaml
Olist_BQ:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: project-olist-470307
      dataset: olist_analytics_dev
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
      retries: 1
      
    prod:
      type: bigquery
      method: service-account
      project: project-olist-470307
      dataset: olist_analytics_prod
      threads: 8
      timeout_seconds: 300
      location: US
      priority: interactive
      retries: 3
      keyfile: /path/to/service-account-key.json
```

### Dataset Structure in BigQuery

```
project-olist-470307/
├── olist_raw/                    # Source data tables
│   ├── customers
│   ├── orders
│   ├── order_items
│   ├── order_payments
│   ├── order_reviews
│   ├── products
│   ├── sellers
│   ├── geolocation
│   └── product_category_name_translation
├── olist_stg/                    # Staging layer
│   └── stg_* tables (9 tables)
├── olist_dwh/                    # Data warehouse layer
│   ├── fact_order_items          # Central fact table
│   └── dim_* tables (8 dimensions)
└── olist_analytics/              # Analytics layer
    └── *_analytics_obt (6 OBT tables)
```

### Authentication Methods

**Development (OAuth)**:
```bash
gcloud auth application-default login
gcloud config set project project-olist-470307
```

**Production (Service Account)**:
1. Create service account in GCP Console
2. Download JSON key file
3. Set environment variable: `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`

---

## 🎯 dbt Data Architecture

### Layer Philosophy

**3-Layer Architecture**: Raw → Staging → Warehouse → Analytics

1. **Staging Layer** (`stg_*`): Clean, standardize, and document raw data
2. **Warehouse Layer** (`dim_*`, `fact_*`): Dimensional modeling for optimal query performance
3. **Analytics Layer** (`*_analytics_obt`): Business-ready denormalized tables

### dbt Project Configuration

**dbt_project.yml** key settings:
```yaml
name: 'Olist_BQ'
version: '1.0.0'
profile: 'Olist_BQ'

models:
  Olist_BQ:
    staging:
      +materialized: table
      +schema: stg
    warehouse:
      +materialized: table  
      +schema: dwh
    analytics_obt:
      +materialized: table
      +schema: analytics
```

### Source Definitions

**Raw data sources** in `models/staging/source.yml`:
- `olist.customers` → Customer master data
- `olist.orders` → Order headers & status
- `olist.order_items` → Line item details
- `olist.order_payments` → Payment transactions
- `olist.order_reviews` → Customer feedback
- `olist.products` → Product catalog
- `olist.sellers` → Seller information
- `olist.geolocation` → Geographic data
- `olist.product_category_name_translation` → Category translations

### Data Quality Framework

**Comprehensive testing strategy**:
- **Source tests**: Freshness, volume checks
- **Staging tests**: Uniqueness, referential integrity, data types
- **Warehouse tests**: Dimensional model integrity
- **Analytics tests**: Business rule validation

---

## 📊 Analytics OBT Layer - Detailed Structure

### Purpose & Design

The Analytics OBT (One Big Table) layer provides **business-ready, denormalized tables** optimized for:
- **BI Tools**: Tableau, Power BI, Looker connectivity
- **Application Development**: Gradio, Flask, Django integration
- **Ad-hoc Analysis**: Data science and exploration
- **Real-time Dashboards**: Streamlit, Plotly Dash

### Complete Table Specifications

#### 1. Revenue Analytics OBT (`revenue_analytics_obt`)
**Purpose**: Comprehensive revenue analysis across all dimensions  
**Grain**: One row per order item with allocated payments  
**Row Count**: ~112,600 records

**Schema Structure**:
```sql
revenue_sk                BIGINT      -- Primary key (surrogate)
order_date               DATE        -- Transaction date for time analysis
customer_state           STRING      -- Geographic revenue segmentation
item_price               NUMERIC     -- Base item price (additive measure)
allocated_payment        NUMERIC     -- Proportionally allocated payment
market_segment          STRING      -- Geographic market classification
shipping_complexity     STRING      -- Logistics complexity tier
```

**Calculated Fields**:
- `market_segment`: ['sao_paulo_market', 'major_southeast', 'south_market', 'other_markets']
- `shipping_complexity`: ['same_state', 'southeast_region', 'south_region', 'cross_region']

**Use Cases**:
- Revenue trending and forecasting
- Geographic market performance analysis
- Shipping cost impact on profitability
- Customer segment contribution analysis

#### 2. Customer Analytics OBT (`customer_analytics_obt`)
**Purpose**: Customer behavior, satisfaction, and lifecycle analysis  
**Grain**: One row per unique customer  
**Row Count**: ~99,441 customers

**Schema Structure**:
```sql
customer_sk              BIGINT      -- Primary key (surrogate)
customer_segment         STRING      -- RFM-based business segmentation
total_spent             NUMERIC     -- Customer lifetime value (CLV)
churn_risk_level        STRING      -- Predictive churn classification
satisfaction_tier       STRING      -- Review-based satisfaction scoring
```

**Segmentation Logic**:
- `customer_segment`: ['champion', 'loyal_customer', 'potential_loyalist', 'new_customer_high_value', 'new_customer_low_value', 'hibernating', 'at_risk', 'needs_attention']
- `churn_risk_level`: ['active', 'low_churn_risk', 'medium_churn_risk', 'high_churn_risk']
- `satisfaction_tier`: ['highly_satisfied', 'satisfied', 'neutral', 'dissatisfied', 'highly_dissatisfied', 'no_feedback']

**Use Cases**:
- Customer segmentation for targeted marketing
- Churn prediction and retention strategies
- Customer lifetime value analysis
- Satisfaction correlation with business metrics

#### 3. Seller Analytics OBT (`seller_analytics_obt`)
**Purpose**: Seller performance evaluation and marketplace insights  
**Grain**: One row per unique seller  
**Row Count**: ~3,095 sellers

**Schema Structure**:
```sql
seller_sk               BIGINT      -- Primary key (surrogate)
performance_tier        STRING      -- Performance-based classification
seller_segment          STRING      -- Business size segmentation
total_revenue           NUMERIC     -- Seller's total generated revenue
```

**Performance Metrics**:
- `performance_tier`: ['top_performer', 'high_performer', 'good_performer', 'average_performer', 'underperformer', 'new_seller']
- `seller_segment`: ['enterprise_seller', 'power_seller', 'professional_seller', 'regular_seller', 'small_seller', 'micro_seller']

**Use Cases**:
- Seller onboarding and tier management
- Commission structure optimization
- Marketplace growth strategies
- Performance-based incentive programs

#### 4. Payment Analytics OBT (`payment_analytics_obt`)
**Purpose**: Payment behavior analysis and installment insights  
**Grain**: One row per payment transaction  
**Row Count**: ~112,600 payment records

**Schema Structure**:
```sql
payment_transaction_sk   BIGINT      -- Primary key (surrogate)
payment_type            STRING      -- Payment method classification
installment_category    STRING      -- Installment behavior grouping
payment_risk_level      STRING      -- Risk assessment classification
customer_payment_profile STRING     -- Customer payment preference
```

**Payment Classifications**:
- `payment_type`: ['credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined']
- `installment_category`: ['single_payment', 'short_installment', 'medium_installment', 'long_installment', 'extended_installment', 'unknown']
- `payment_risk_level`: ['minimal_risk', 'low_risk', 'medium_risk', 'high_risk']

**Use Cases**:
- Payment method optimization
- Installment plan effectiveness
- Financial risk assessment
- Payment gateway cost analysis

#### 5. Geographic Analytics OBT (`geographic_analytics_obt`)
**Purpose**: Geographic market analysis and regional performance  
**Grain**: One row per Brazilian state  
**Row Count**: 27 Brazilian states

**Schema Structure**:
```sql
state_code                  STRING      -- Primary key (BR state code)
market_development_tier     STRING      -- Market maturity classification
geographic_region          STRING      -- Brazilian geographic regions
market_tier                STRING      -- Market importance stratification
```

**Geographic Classifications**:
- `market_development_tier`: ['tier_1_developed', 'tier_2_growing', 'tier_3_emerging', 'tier_4_developing', 'tier_5_nascent']
- `geographic_region`: ['southeast', 'south', 'center_west', 'northeast', 'north', 'unknown']
- `market_tier`: ['tier_1_sao_paulo', 'tier_1_major_southeast', 'tier_2_south', 'tier_2_major_regional', 'tier_3_secondary', 'tier_4_emerging']

**Use Cases**:
- Market expansion planning
- Regional performance benchmarking
- Logistics optimization
- Marketing budget allocation

#### 6. Delivery Analytics OBT (`delivery_analytics_obt`)
**Purpose**: Delivery performance monitoring and logistics insights  
**Grain**: One row per order with delivery tracking  
**Row Count**: ~112,600 delivery records

**Schema Structure**:
```sql
delivery_transaction_sk     BIGINT      -- Primary key (surrogate)
delivery_performance_tier   STRING      -- Overall delivery performance
delivery_speed_tier        STRING      -- Speed classification
delivery_accuracy_tier     STRING      -- Timing accuracy vs estimates
order_status              STRING      -- Current fulfillment status
```

**Delivery Classifications**:
- `delivery_performance_tier`: ['outstanding', 'good', 'average', 'poor', 'unclassified']
- `delivery_speed_tier`: ['excellent', 'good', 'average', 'poor', 'very_poor', 'unknown']
- `delivery_accuracy_tier`: ['early_delivery', 'on_time_delivery', 'slightly_late', 'late_delivery', 'very_late', 'no_estimate']

**Use Cases**:
- Logistics performance optimization
- Carrier evaluation and selection
- Delivery time prediction modeling
- Customer experience improvement

### Integration Patterns for External Applications

#### For Gradio Applications:
```python
# Example connection pattern
from google.cloud import bigquery

client = bigquery.Client(project='project-olist-470307')

# Query analytics OBT for ML features
query = """
SELECT customer_sk, customer_segment, total_spent, churn_risk_level
FROM `project-olist-470307.olist_analytics.customer_analytics_obt`
WHERE churn_risk_level IN ('medium_churn_risk', 'high_churn_risk')
"""

df = client.query(query).to_dataframe()
```

#### For API Development:
```python
# FastAPI endpoint example
@app.get("/analytics/revenue/{state}")
async def get_revenue_by_state(state: str):
    query = f"""
    SELECT market_segment, SUM(allocated_payment) as total_revenue
    FROM `project-olist-470307.olist_analytics.revenue_analytics_obt`
    WHERE customer_state = '{state}'
    GROUP BY market_segment
    """
    return query_bigquery(query)
```

---

## 🎨 Streamlit Dashboard

### Dashboard Architecture

**Multi-page Application** with specialized analytics modules:

#### Main Dashboard (`main.py`)
- **Overview**: Key business metrics and navigation
- **Real-time Status**: Data freshness and system health
- **Quick Insights**: Summary cards and trending indicators

#### Analytics Pages

**1. 📈 Revenue Analytics** (`1_📈_Revenue_Analytics.py`)
- **Revenue Trends**: Time series analysis with seasonal decomposition
- **Geographic Revenue**: State-level performance heatmaps
- **Market Segmentation**: Revenue contribution by market segments
- **Shipping Impact**: Cost analysis and profitability insights
- **Interactive Filters**: Date range, state selection, market segments

**2. 👥 Customer Analytics** (`2_👥_Customer_Analytics.py`)
- **Customer Segmentation**: RFM analysis and segment distribution
- **Lifetime Value**: CLV trends and segment comparison
- **Churn Analysis**: Risk prediction and retention metrics
- **Satisfaction Insights**: Review sentiment and correlation analysis
- **Cohort Analysis**: Customer behavior over time

**3. 🏪 Seller Analytics** (`3_🏪_Seller_Analytics.py`)
- **Performance Tiers**: Seller classification and distribution
- **Revenue Distribution**: Top performers and long-tail analysis
- **Geographic Spread**: Seller location and market coverage
- **Growth Metrics**: New seller onboarding and progression
- **Marketplace Health**: Concentration and diversity metrics

**4. 💳 Payment Analytics** (`4_💳_Payment_Analytics.py`)
- **Payment Methods**: Usage patterns and preferences
- **Installment Analysis**: Payment behavior and risk assessment
- **Financial Metrics**: Transaction values and processing costs
- **Risk Assessment**: Payment failure patterns and mitigation
- **Trend Analysis**: Payment evolution over time

**5. 🗺️ Geographic Analytics** (`5_🗺️_Geographic_Analytics.py`)
- **Market Development**: Tier classification and growth potential
- **Regional Performance**: Revenue and activity by region
- **Expansion Opportunities**: Underserved markets identification
- **Logistics Insights**: Delivery performance by geography
- **Interactive Maps**: Choropleth visualizations with drill-down

**6. 🚚 Delivery Analytics** (`6_🚚_Delivery_Analytics.py`)
- **Performance Metrics**: Speed, accuracy, and overall performance
- **Delivery Trends**: Seasonal patterns and improvement tracking
- **Logistics Optimization**: Route efficiency and cost analysis
- **Customer Experience**: Delivery satisfaction correlation
- **Operational Insights**: Bottleneck identification and resolution

### Visualization Technologies

**Plotly Integration**:
- **Interactive Charts**: Zoom, filter, hover, and drill-down capabilities
- **Geographic Maps**: Choropleth and scatter maps for spatial analysis
- **Time Series**: Advanced trending with annotations and forecasting
- **Distribution Plots**: Histograms, box plots, and violin plots
- **Correlation Analysis**: Heatmaps and scatter matrix visualizations

**Streamlit Components**:
- **Metrics Cards**: KPI display with delta indicators
- **Sidebar Filters**: Dynamic filtering across all visualizations
- **Column Layouts**: Responsive grid system for optimal viewing
- **Progress Indicators**: Real-time loading and processing feedback
- **Export Functionality**: Download charts and data extracts

---

## 🚀 Quick Start Guide

### 1. Environment Setup
```bash
# Clone repository
git clone <repository-url>
cd Olist_BQ

# Setup environment
conda env create -f environment.yml
conda activate olist_analytics
```

### 2. GCP Authentication
```bash
# Authenticate with Google Cloud
gcloud auth application-default login
gcloud config set project project-olist-470307

# Verify BigQuery access
bq ls project-olist-470307:
```

### 3. dbt Setup & Execution
```bash
# Navigate to dbt directory
cd dbt/

# Install dbt dependencies
dbt deps

# Test source connections
dbt source freshness

# Run staging models
dbt run --models staging

# Run warehouse models
dbt run --models warehouse

# Run analytics models
dbt run --models analytics_obt

# Run all tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### 4. Streamlit Dashboard Launch
```bash
# Navigate to streamlit directory
cd ../streamlit/

# Launch dashboard
streamlit run main.py
```

### 5. Verification Steps
1. **dbt Documentation**: Access at `http://localhost:8080`
2. **Streamlit Dashboard**: Access at `http://localhost:8501`
3. **BigQuery Console**: Verify table creation in `project-olist-470307`
4. **Data Quality**: Review test results in dbt output

---

## 🧪 Development & Testing

### dbt Development Workflow

**Model Development**:
```bash
# Develop specific model
dbt run --select model_name

# Test during development
dbt test --select model_name

# Full refresh if needed
dbt run --full-refresh --select model_name
```

**Testing Strategy**:
- **Generic Tests**: `not_null`, `unique`, `accepted_values`, `relationships`
- **Custom Tests**: Business rule validation in `tests/` directory
- **Source Tests**: Data freshness and volume monitoring
- **Schema Tests**: Column presence and data type validation

### Data Quality Monitoring

**Automated Checks**:
- **Freshness Tests**: Ensure data is updated within SLA
- **Volume Tests**: Detect significant data volume changes
- **Referential Integrity**: Maintain foreign key relationships
- **Business Rules**: Custom validation for domain logic

### Performance Optimization

**BigQuery Optimization**:
- **Partitioning**: Date-based partitioning on time dimensions
- **Clustering**: Optimize for common filter patterns
- **Materialization**: Table vs view trade-offs by layer
- **Query Optimization**: Avoid SELECT * and optimize JOINs

### Deployment Patterns

**Environment Promotion**:
```bash
# Development
dbt run --target dev

# Production deployment
dbt run --target prod
```

**CI/CD Integration**:
- **GitHub Actions**: Automated testing and deployment
- **dbt Cloud**: Production scheduling and monitoring
- **Data Quality Gates**: Prevent deployment with test failures

---

## 📚 Additional Resources

### Documentation Links
- **dbt Documentation**: Auto-generated at `dbt docs serve`
- **Streamlit App**: Interactive dashboard with built-in help
- **BigQuery Console**: Data exploration and query development
- **Project Wiki**: Advanced configuration and troubleshooting

### Support & Contributions
- **Issues**: Report bugs and feature requests via GitHub Issues
- **Contributions**: Follow standard git flow for pull requests
- **Code Style**: Black formatting, isort imports, flake8 linting
- **Documentation**: Update README.md and dbt model documentation

---

**Last Updated**: September 2025  
**Project Version**: 1.0.0  
**dbt Version**: 1.6.0+  
**Python Version**: 3.11+
